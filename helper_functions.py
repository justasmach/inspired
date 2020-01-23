###### import everything upwards from home dir
from __future__ import absolute_import
import psycopg2, argparse, six, re, datetime, json, yaml, sys, io, os, xlrd, time
from xlrd import XLRDError
from configparser import ConfigParser
from os import path
import psycopg2.extras
import pandas as pd
import numpy as np
import smtplib
import requests
import httplib2
import urllib.request
import google.ads.google_ads.client
from googleapiclient.discovery import build
from googleapiclient import http
from facebook_business.api import FacebookAdsApi
from facebook_business.api import FacebookRequestError
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebookads.adobjects.adsinsights import AdsInsights
from ast import literal_eval
from simplejson import JSONDecodeError
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials
from func_timeout import func_timeout, FunctionTimedOut


# ---------------- DATABASE FUNCTIONS ----------------


# get all the db configuration from file
def db_config(filename, section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

# initiate db connection
def init_conn():
    conn = None
    base, tail = os.path.split(os.getcwd())
    filename = path.join(base, 'database.ini')
    params = db_config(filename, 'postgresql')
    conn = psycopg2.connect(**params)
    return conn

# close db connection
def end_conn(conn):
    conn.close()

# initiate db cursor
def init_cur(conn):
    cur = conn.cursor()
    return cur  

# close cursor
def end_cur(cur):
    cur.close()

# create table using dynamic str
def create_table(conn, cur, tab_cr_str):
    cur.execute(tab_cr_str)
    conn.commit()
    
# drop table if defined by user at start of execution
def drop_table(conn, cur, t_name, do_drop):
    if do_drop:
        cur.execute(f"DROP TABLE IF EXISTS {t_name};")
    conn.commit()

# using upsert_str to do a batch commit. better performace than row by row 
def upsert(conn, cur, upsert_q, df_res_tuple, page_size):
    psycopg2.extras.execute_values(cur, upsert_q, df_res_tuple)
    conn.commit()
    
# dynamic table creation str for create_table function
def tab_creation_str(t_name, pk_name, pk_lst, dtype_dict):
    result = ''
    columns = ''
    for key in dtype_dict:
        columns = columns + key + ' ' + dtype_dict[key] + ', '
    query_str = ("CREATE TABLE IF NOT EXISTS " + t_name + " " + 
                "(" + columns + "CONSTRAINT " 
               + pk_name + " PRIMARY KEY (" + ', '.join(pk_lst) + "));")
    return query_str

# add column if not exists, used when creating the table
# and when making multiple calls if additional columns are received
def add_missing_cl(conn, cur, t_name, str_full):
    if str_full != None:
        cur.execute(f"ALTER TABLE {t_name} {str_full};")
    conn.commit()

# get present columns in the table if table already exists
def get_db_cols(cur, t_name):
    exists = cur.execute(f"SELECT to_regclass('{t_name}');")
    column_names = []
    if exists != None:
        cur.execute(f"SELECT * FROM {t_name} WHERE False")
        column_names = [desc[0] for desc in cur.description]
    return column_names 

# ---------------- REGEX ----------------


# use regex to extract PLN from a single string
def pln_no_reg(campaign_name):
    pln_no = str(re.findall('(PLN?[\-]\d{1,4}?[\-]\d{1,4})', campaign_name))[2:-2]
    return pln_no

# use regex to extract only the campaign name from a single string
def name_cl_reg(campaign_name):
    pln = str(re.findall('(PLN?[\-]\d{1,4}?[\-]\d{1,4})', campaign_name))[2:-2]
    name_cl = campaign_name.replace(pln, '')
    name_cl = str(re.findall('^[^a-zA-Z\d]*(.*)', name_cl))[2:-2]
    return name_cl

# ---------------- STRING OPERATIONS ----------------


# relpace numpy nan values with generic python nulls
def rplc_nan(df_response):
    df_response = df_response.replace({pd.np.nan: None})
    return df_response

# rename column headers if any unsupported chars are found, due to db limitations
def rename_col(col_name): 
    char_dct = {".": "_",
                ":": "_",
                " ": "_",
                "(": "_",
                ")": "_",
                "-": "_",
                "%": "_",
                ",": "_",
                ":": "_",
                "'": "_",
                "/": "_",
                "\\": "_",
                "<=": "_less_th_or_eq_to_",
                ">=": "_more_th_or_eq_to_",
                "<": "_less_than_",
                ">": "_more_than_",
                "=": "_eq_to_",
                "+": "_plus_"}
    for key in char_dct:
        if key in col_name:
            col_name = col_name.replace(key, char_dct[key])
    if col_name[0].isdigit() or col_name.lower() == 'order' or col_name.lower() == 'group':
        col_name = ('_' + col_name)
    return col_name

# dynamic update or insert string, updates everything except for creation_ts
def upsert_str(dims, t_name, pk):
    upd_str = (' SET ')
    upd_str_dim = ''
    for dim in dims:
        if dim != 'creation_ts':
            str_tmp = f"{dim} = EXCLUDED.{dim}, "
            upd_str_dim = upd_str_dim + str_tmp
    upd_str = str(upd_str + upd_str_dim)[1:-2] + ';'
    upsert_q = (f"INSERT INTO {t_name} ({', '.join(dims)}) "
                        "VALUES %s "
                        f"ON CONFLICT ({pk}) " 
                            f"DO "
                                f"UPDATE "
                                f"{upd_str} ")
    return str(upsert_q)

# dynamic missing column string, adds columns if missing between API calls
def add_missing_cl_str(dtype_dict, db_cols):
    str_full = ''
    if len(dtype_dict) != len(db_cols):
        str_part = 'ADD COLUMN IF NOT EXISTS '
        for key in dtype_dict:
            str_full = str_full + str_part + key + ' ' + dtype_dict[key] + ', '
        str_full = str_full[:-2]     
    return str_full

# ---------------- DATETIME OPERATIONS ----------------

# return formatted period for last 90 days
def upd_last_90(period_xlsx, per_format):     
    if isinstance(period_xlsx, list):
        check_str = str(period_xlsx[0])
    else:
        check_str = period_xlsx
    if check_str == 'upd_last_90':
        end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        if per_format == 'lst':
            return start_date, end_date
        else:
            period_xlsx = per_format.replace('x1', start_date)
            period_xlsx = period_xlsx.replace('x2', end_date)
            return period_xlsx
    else:
        return period_xlsx

# split received period to defined intervals, currently only for facebook
def period_split(def_period, def_intv):
    start = datetime.strptime(list(def_period.values())[0],"%Y-%m-%d")
    end = datetime.strptime(list(def_period.values())[1],"%Y-%m-%d")
    diff = (end  - start ) / def_intv
    period_lst = []
    curr = start
    period_lst.append(start.strftime("%Y-%m-%d"))
    while True:
        curr = curr + timedelta(days = def_intv)
        if curr >= end:
            break
        else:
            period_lst.append(curr.strftime("%Y-%m-%d"))
    period_lst.append(end.strftime("%Y-%m-%d"))
    return period_lst

# ---------------- DATAFRAME OPERATIONS ----------------


# apply pln_no_reg and insert new column to dataframe with the clean PLN
def get_pln_no(df_response, src_col_name, is_in_df):
    if is_in_df and src_col_name in df_response.columns:
        df_response.insert(0, 'pln_no', df_response[src_col_name].apply(pln_no_reg))
    return df_response

# apply name_cl_reg and insert new column to dataframe with the clean campaign name 
def get_name_col(df_response, src_col_name, is_in_df):
    if is_in_df and src_col_name in df_response.columns:
        df_response.insert(0, 'campaing_name_noPLN', df_response[src_col_name].apply(name_cl_reg))
    return df_response

# add timestamps for every row in the dataframe
def add_ts(dataframe):
    dataframe.insert(0, 'creation_ts', datetime.now())
    dataframe.insert(0, 'last_updated_ts', datetime.now())
    return dataframe 

# get data types for every column
def get_types(df_response):   
    col_names = df_response.head(0).to_dict()
    col_dtypes = {}
    for key in col_names:
        is_ts = False
        is_date = False
        is_real = False
        is_int = False
        is_str = False
        dtype = ''
        # iterates through every column and row, to assign proper data type (includes exceptions)
        for i, row in df_response.iterrows():
            try:
                if row[key] is None:
                    pass
                elif re.match('\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d+', str(row[key])):
                    is_ts = True
                    break
                elif (re.match('\d{4}\-\d{2}\-\d{2}', str(row[key])) and 'keyword' not in str(key).lower()) or 'ga_date' in str(key).lower():
                    is_date = True
                    break
                elif 'float' in str(type(literal_eval(str(row[key])))) or str(key).lower() == 'value' or str(key).lower() == 'spend' or 'd_view' in str(key).lower() or 'd_click' in str(key).lower():
                    is_real = True
                    break
                elif 'int' in str(type(literal_eval(str(row[key])))):
                    is_int = True
            except (ValueError, SyntaxError):
                is_str = True
                pass
        if is_ts and not is_str:
            dtype = 'TIMESTAMP'
        elif is_date and not is_str:
            dtype = 'DATE'
        elif is_real and not is_str:
            dtype = 'REAL'
        elif is_int and not is_str:
            dtype = 'BIGINT'
        else:
            dtype = 'VARCHAR'
        col_dtypes.update({key : dtype})
    return col_dtypes

# ---------------- OTHER ----------------

# create log entry for row passed
def log_string(platform, text):
    now = datetime.now()
    curr_date = now.strftime('%m_%d_%Y')
    timestamp = now.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    filename = platform + '_' + 'log' + '_' + curr_date + '.txt'
    
    if os.path.exists(filename):
        append_write = 'a'
    else:
        append_write = 'w'
    std_copy = open(filename, append_write)
    std_copy.write('[' + timestamp + ']:    ')
    std_copy.write(str(text))
    std_copy.write('\n')

# ---------------- MAIN ----------------

# main method which calls required functions in sequence
def postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm):
    try:
        df_response = df_response.rename(columns=rename_col)
        df_response = get_pln_no(df_response, src_col_name, is_pln_df)
        df_response = get_name_col(df_response, src_col_name, is_pln_df)
        df_response = rplc_nan(df_response)
        df_response = add_ts(df_response)
        dtype_dict = get_types(df_response)
        conn = init_conn()
        cur = init_cur(conn)
        db_cols = get_db_cols(cur, t_name)
        drop_table(conn, cur, t_name, do_drop)
        tab_cr_str = tab_creation_str(t_name, pk_name, pk_lst, dtype_dict)
        create_table(conn, cur, tab_cr_str)

        missing_cl_str = add_missing_cl_str(dtype_dict, db_cols)
        add_missing_cl(conn, cur, t_name, missing_cl_str)
        
        df_res_tuple = list(df_response.itertuples(index=False, name=None))
        upsert_q = upsert_str(dtype_dict.keys(), t_name, (','.join(pk_lst)))
        upsert(conn, cur, upsert_q, df_res_tuple, page_size)    
        
        end_cur(cur)
        end_conn(conn)
    except(Exception, psycopg2.DatabaseError) as error:
        out_str = 'Database error'
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()
    out_str = 'Database connection closed.'
    print(out_str)
    log_string(log_pltfrm, out_str)