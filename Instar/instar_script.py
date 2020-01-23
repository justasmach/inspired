#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def read_txt(log_pltfrm, file_dir, file, start_pos, enc):
    out_str = 'Reading...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    try:
        # read data from xlsx
        filepath = file_dir + file
        df_plan = pd.read_csv(filepath, sep='	', header = start_pos, encoding = enc)
        if len(df_plan.index) == 0:
            raise KeyError('No data')
        
    except(NameError, XLRDError, KeyError, UnicodeDecodeError) as error:
        out_str = ('Error while reading file')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)
    return df_plan

def mailer(message):
    cred_file = open("mail_credentials.txt", "r")
    platform = 'Instar'
    user, password, receiver_lst = cred_file.readline().split(":")
    mailserver = smtplib.SMTP('smtp.office365.com',25)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login(user, password)
    message = """From: <%s>
To: <%s>
Subject: %s warning

System time: %s

Error message(s)
%s
""" % (user, receiver_lst, platform, datetime.now(), message)
    mailserver.sendmail(user, list(receiver_lst.split(",")) , message)
    mailserver.quit()
def db_del(df_plan, t_name):
    conn = init_conn()
    cur = init_cur(conn)
    pln_id = (str(df_plan.iat[0,0]))
    exists = cur.execute(f"SELECT to_regclass('public.{t_name}');")
    if exists != None:
        cur.execute(f"DELETE FROM {t_name} WHERE PLN_ID = {pln_id};")
    end_cur(cur)
    end_conn(conn)

try:
    file_dir = 'Z:\\09.Reporting_Dep\\Default_Data\\InfoSys\\'
    log_pltfrm = 'instar'
    do_drop = False
    file_lst = [
#                 'Last_day_All_Spots.txt'
#                 ,'Last_week_All_Spots.txt'
#                 ,'LastDayTiimeBands.txt'
#                 ,'LastWeekTiimeBands.txt'
                '2016_timeBands.txt'
                ,'2017_timeBands.txt'
                ,'2018_timeBands.txt'
                ,'2019_timeBands.txt'
                ,'2020_timeBands.txt'
                ,'All_Spots_2016.txt'
                ,'All_Spots_2017.txt'
                ,'All_Spots_2018.txt'
                ,'All_Spots_2019.txt'
                ,'All_Spots_2020.txt'
                ]
    for file in file_lst:
        out_str = ('File: ' + file)
        print(out_str)
        log_string(log_pltfrm, out_str)
        df_plan = pd.DataFrame()
        date_range = []
        date_col_uniq =[]
        t_name =''
        pk_name = ''
        pk_list = []
        if 'all_spots' in file.lower():
            enc = 'ISO-8859-1'
            start_pos = 2
            df_plan = read_txt(log_pltfrm, file_dir, file, start_pos, enc)
            df_plan = df_plan.drop(df_plan.index[0])      
            
            date_range = pd.date_range(start=df_plan.Date.min(), end=df_plan.Date.max(), freq='D')
            df_plan['Date'] =  pd.to_datetime(df_plan['Date'], infer_datetime_format=True)
            date_col_uniq = df_plan.Date.unique()
            
            t_name = 'instar_all_spots'
            pk_name = 'instar_all_spots_pk'
            pk_lst = ['Campaign', 'Channel', 'Date', 'Start_time', 'Brand', 'Advertiser']
            
        elif 'tiimebands' in file.lower() or 'timebands' in file.lower():
            enc = 'utf-8'
            start_pos = 2
            df_plan = read_txt(log_pltfrm, file_dir, file, start_pos, enc)
            df_plan[['Timeband_start','Timeband_end']] = df_plan.Timebands.str.split(" - ", expand=True)
            df_plan.drop('Timebands', axis=1, inplace=True)  
            
            date_range = pd.date_range(start=df_plan.Dates.min(), end=df_plan.Dates.max(), freq='D')
            df_plan['Dates'] =  pd.to_datetime(df_plan['Dates'], infer_datetime_format=True)
            date_col_uniq = df_plan.Dates.unique()
            
            t_name = 'instar_time_bands'
            pk_name = 'instar_time_bands_pk'
            pk_lst = ['Channels', 'Dates', 'Timeband_start', 'Timeband_end']
            
        page_size = 10000
        src_col_name = 'PLN_ID'
        is_pln_df = False
        add_name_cl = False
        
        result_str = ''
        is_missing = False
        for date in date_range:
            if not str(date.date()) in str(date_col_uniq):
                out_str = f'Date is missing: {date.date()} for file: {file}!'
                print(out_str)
                log_string(log_pltfrm, out_str)
                result_str = result_str + out_str + '\n'
                is_missing = True
        if is_missing:
            mailer(result_str)
            out_str = ('Mail sent')
            print(out_str)
            log_string(log_pltfrm, out_str)
        
        row_count = len(df_plan.index)
        out_str = ('Row count: ' + str(row_count))
        print(out_str)
        log_string(log_pltfrm, out_str)
        postgre_write_main(df_plan, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)

except KeyError as error:
    out_str = ('Key Error')
    print(out_str)
    log_string(log_pltfrm, out_str)
    print(error)
    log_string(log_pltfrm, error)