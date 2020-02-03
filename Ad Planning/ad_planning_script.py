#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def read_xlsx(log_pltfrm, file_dir, file):
    out_str = 'Starting...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    try:
        # read data from xlsx
        filepath = file_dir + '\\' + file
        df_plan = pd.read_excel(filepath, sheet_name='Sheet1', header=0)
        if len(df_plan.index) == 0:
            raise KeyError('No data')
    except(NameError, XLRDError, KeyError) as error:
        out_str = ('Error while reading file')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)
    return df_plan

def db_del(df_plan, t_name, pln_id):
    conn = init_conn()
    cur = init_cur(conn)

    cur.execute(f"SELECT to_regclass('public.{t_name}');")
    row1 = cur.fetchone()
    exists = row1[0]
    if exists != None:
        out_str = (f'Deleting all existing rows with PLN_ID: {pln_id} and inserting new ones from file.')
        print(out_str)
        log_string(log_pltfrm, out_str)
        cur.execute(f"DELETE FROM {t_name} WHERE PLN_ID = '{pln_id}';")
    else:
        out_str = (f'Table {t_name} does not exist and will be created.')
        print(out_str)
        log_string(log_pltfrm, out_str)
    end_cur(cur)
    end_conn(conn)

try:
    file_dir = r'\\srvfiles.inspired.local\Book\DataBase\Internet'
    log_pltfrm = 'ad_planning'
    
    out_str = ('File path: ' + file_dir)
    print(out_str)
    log_string(log_pltfrm, out_str)
    
    do_drop = False
    
    if os.listdir(file_dir):
        out_str = ('Files in directory: ' + str(os.listdir(file_dir)))
    else:
        out_str = ('Directory is empty!')
    print(out_str)
    log_string(log_pltfrm, out_str)
    
    for file in os.listdir(file_dir):
        out_str = ('File name: ' + file)
        log_string(log_pltfrm, out_str)
        print(out_str)
        if file and file.endswith('.xlsx'):
            
            out_str = ('File is in .xlsx format')
            print(out_str)
            log_string(log_pltfrm, out_str)
            
            df_plan = read_xlsx(log_pltfrm, file_dir, file)
            pln_id = (str(df_plan.iat[0,0]))
            
            row_count = len(df_plan.index)
            
            out_str = ('Row count: ' + str(row_count))
            print(out_str)
            log_string(log_pltfrm, out_str)
            
            t_name = 'ad_plan'
            pk_name = 'ad_plan_pk'
            pk_lst = ['PLN_ID', 'PLN_No', 'Client', 'Brand', 'Product', 'Project', 'Provider', 'Portal', 'Position', 'Capping', 'Banners', 'Banner_Type', 'Date', 'Buying_Type', 'Discount', 'DB', 'Agency_Buying_Type']
            page_size = 100000
            src_col_name = 'PLN_ID'
            is_pln_df = False
            add_name_cl = False
            
            if row_count > 0:
                db_del(df_plan, t_name, pln_id)
                postgre_write_main(df_plan, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)
                out_str = (f'Data for PLN_ID: {pln_id} written to DB.')
                print(out_str)
                log_string(log_pltfrm, out_str)
            
            if os.path.exists(file_dir + '\\' + file):
                os.remove(file_dir + '\\' + file)
                out_str = ('File removed')
                print(out_str)
                log_string(log_pltfrm, out_str)
            else:
                print("The file does not exist")
        else:
            continue
    
except KeyError as error:
    out_str = ('Key Error')
    print(out_str)
    log_string(log_pltfrm, out_str)
    print(error)
    log_string(log_pltfrm, error)