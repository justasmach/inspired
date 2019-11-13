###### import everything upwards from home dir
from __future__ import absolute_import
import psycopg2
import psycopg2.extras
from configparser import ConfigParser
import argparse
import six
import sys
import pandas as pd
import re
import datetime
import numpy as np
import json
import yaml
from xlrd import XLRDError
from facebook_business.api import FacebookAdsApi
from facebook_business.api import FacebookRequestError
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebookads.adobjects.adsinsights import AdsInsights
import threading
from ast import literal_eval
from datetime import datetime, timedelta
import io
import google.ads.google_ads.client
from googleapiclient.discovery import build
from googleapiclient import http
from oauth2client.service_account import ServiceAccountCredentials


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

def create_table(tab_cr_str, conn, cur):
    cur.execute(tab_cr_str)
    conn.commit()
    
def tab_creation_str(t_name, pk_name, pk_lst, dtype_dict):
    result = ''
    columns = ''
    for key in dtype_dict:
        columns = columns + key + ' ' + dtype_dict[key] + ', '
    query_str = ("CREATE TABLE IF NOT EXISTS " + t_name + " " + 
                "(" + columns + "CONSTRAINT " 
               + pk_name + " PRIMARY KEY (" + ', '.join(pk_lst) + "));")
    return query_str, dtype_dict

def init_conn():
    conn = None
    params = db_config('database.ini', 'postgresql')
    conn = psycopg2.connect(**params)
    return conn

def rplc_nan(df_response):
    df_response = df_response.replace({pd.np.nan: None})
    return df_response

def end_conn(conn):
    conn.close()
    
def init_cur(conn):
    cur = conn.cursor()
    return cur
    
def add_column(t_name, db_dim, col_dtype, cur):
    cur.execute(f"ALTER TABLE {t_name} "
                f"ADD COLUMN IF NOT EXISTS {db_dim} {col_dtype};")

def drop_table(t_name, do_drop, cur):
    if do_drop:
        cur.execute(f"DROP TABLE IF EXISTS {t_name};")
    
def add_ts(dataframe):
    dataframe.insert(0, 'creation_ts', datetime.now())
    dataframe.insert(0, 'last_updated_ts', datetime.now())
    return dataframe 

def get_pln_no(df_response, src_col_name, is_in_df):
    if is_in_df:
        df_response.insert(0, 'pln_no', df_response[src_col_name].apply(pln_no_reg))
    return df_response
    
def pln_no_reg(campaign_name):
    return str(re.findall('(PLN?[\-]\d{1,4}?[\-]\d{1,4})', campaign_name))[2:-2]

def rename_col(col_name):
    if '.' in col_name:
        return col_name.replace('.', '_')
    elif ':' in col_name:
        return col_name.replace(':', '_')
    elif col_name[0].isdigit():
        return ('_' + col_name)
    else:
        return col_name
    
def get_types(df_response):   
    col_names = df_response.head(0).to_dict()
    col_dtypes = {}
    dtype = ''
    for key in col_names:
            for i, row in df_response.iterrows():
                try:
                    
                    dtype = ''
                    if re.match('\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d+', str(row[key])):
                        dtype = 'TIMESTAMP'
                        break
                    elif (re.match('\d{4}\-\d{2}\-\d{2}', str(row[key])) and 'keyword' not in str(key).lower()) or 'ga_date' in str(key).lower():
                        dtype = 'DATE'
                        break
                    elif 'float' in str(type(literal_eval(row[key]))):
                        dtype = 'REAL'
                        break
                    else:
                        dtype = 'BIGINT'
                except (ValueError, SyntaxError):
                    dtype = 'VARCHAR'
            col_dtypes.update({key : dtype})
    return col_dtypes
        
def upsert_str(dims, t_name, pk):
    upsert_q = (f"INSERT INTO {t_name} ({dims}) "
                        "VALUES %s "
                        f"ON CONFLICT ({pk}) " 
                            f"DO "
                                f"UPDATE "
                                f"SET creation_ts = EXCLUDED.creation_ts; ")
    return str(upsert_q)

def upsert(conn, cur, upsert_q, df_res_tuple, page_size):
    psycopg2.extras.execute_values(cur, upsert_q, df_res_tuple)
    conn.commit()
    cur.close()

def postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df):
    try:
        df_response = df_response.rename(columns=rename_col)
        df_response = get_pln_no(df_response, src_col_name, is_pln_df)
        df_response = rplc_nan(df_response)
        df_response = add_ts(df_response)
        dtype_dict = get_types(df_response)
        conn = init_conn()
        cur = init_cur(conn)
        drop_table(t_name, do_drop, cur)
        tab_creation = tab_creation_str(t_name, pk_name, pk_lst, dtype_dict)
        tab_cr_str = tab_creation[0]
        tab_types = tab_creation[1]
        create_table(tab_cr_str, conn, cur)

        df_res_tuple = list(df_response.itertuples(index=False, name=None))
        upsert_q = upsert_str((','.join(dtype_dict.keys())), t_name, (','.join(pk_lst)))
        upsert(conn, cur, upsert_q, df_res_tuple, page_size)    

        end_conn(conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Database error')
        print(error)
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()
    print('Database connection closed.')
    print('')

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