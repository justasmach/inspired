#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def google_marketing(profile_id, report_id, key_file_location, scopes, log_pltfrm):

    # Initializes an Analytics Reporting API V4 service object.
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
                key_file_location, scopes)
    except(NameError, IOError, FileNotFoundError) as error:
        out_str = 'Could not read configuration file(s)'
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)   

    # Build the service object.
    service = build('dfareporting', 'v3.3', credentials=credentials)
    
    # define empty pandas dataframe
    df_response = pd.DataFrame()

    out_str = 'Calling GMP API...'
    log_string(log_pltfrm, out_str)
    try:
        report_ID = report_id
        response = service.reports().run(profileId = profile_id, reportId=report_ID).execute()
        
        file_ID = response['id']
        
        import time
        #import simplejson
        from googleapiclient.errors import HttpError
        
        # Wait for the report file to finish processing.
        sleep = 30
        start_time = time.time()
        MAX_RETRY_ELAPSED_TIME = 3600
        while True:
            # Make the request
            report_file = service.files().get(
                reportId=report_ID, fileId=file_ID).execute()

            status = report_file['status']
            # Check status, if processing - wait, if ready - save to dataframe
            if status == 'REPORT_AVAILABLE':
                csv_raw = []
                
                request_file = service.files().get_media(reportId=report_ID, fileId=file_ID)

                csv_raw.append(str(request_file.execute()))
                csv_string = '\n'.join(csv_raw)

                # Turning csv format data into 2-D arrays
                data = csv_string.split('\\n')
                data = [i.split(",") for i in data]
                data[0][0] = data[0][0].replace("b'","")

                headers = -1
                data_start = -1
                data_matrx = []
                header_lst = []
                for i, row in enumerate(data, start=0):
                    if row and 'Grand Total:' not in row:
                        if 'Report Fields' in row:
                            headers = i + 1
                            data_start = headers + 1
                        if i == headers:
                            header_lst = data[i]
                        if i >= data_start:
                            data_matrx.append(row)
                df_response = pd.DataFrame(data = data_matrx, columns = header_lst)
                df_response = df_response.dropna(axis = 0, how = 'any')
                break
            elif status != 'PROCESSING':
                print('File status is %s, processing failed.' % status)
            elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                print('File processing deadline exceeded.')

            print('File status is %s, sleeping for %d seconds.' % (status, sleep))
            time.sleep(sleep)
    
        out_str = (str(len(df_response.index)) + ' row(s) received')
        print(out_str)
        log_string(log_pltfrm, out_str)
        
    except(http.HttpError) as error:
        print('GMP API error')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)  
        log_string(log_pltfrm, error)
        sys.exit(1)
    return df_response


def gmp_prep(log_pltfrm):
    out_str = 'Starting...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    do_drop = False
    try:
        # read configuration from excel
        df_conf_base = pd.read_excel('google_marketing_platform_conf_1.xlsx', sheet_name='base', header=0)
        if len(df_conf_base) == 0:
            raise KeyError('No base data provided (profile_id, report_id)')

        key_file_location = 'secrets.json'
        scopes = ['https://www.googleapis.com/auth/dfareporting', 'https://www.googleapis.com/auth/dfatrafficking', 'https://www.googleapis.com/auth/ddmconversions']
    except(NameError, XLRDError, KeyError) as error:
        out_str = ('Error while reading configuration file(s)')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)      

    # iterate over view IDs
    for index, row in df_conf_base.iterrows():
        try:
            profile_id = str(int(row['profile_id']))
            out_str = ('Profile ID: ' + profile_id)
            print(out_str)
            log_string(log_pltfrm, out_str)
            report_id = str(int(row['report_id']))
            out_str = ('Report ID: ' + report_id)
            print(out_str)
            log_string(log_pltfrm, out_str)
        except(KeyError, ValueError) as error:
            out_str = 'Could not read column'
            print(out_str)
            log_string(log_pltfrm, out_str)
            print(error)
            log_string(log_pltfrm, error)
            sys.exit(1)
        
        # call defined methods
        google_marketing_response = google_marketing(profile_id, report_id, key_file_location, scopes, log_pltfrm)

        t_name = 'google_marketing_platform_new'
        pk_name = 'gmp_new_pk'
        pk_lst = ['Campaign', 'Campaign_ID', 'Site__DCM_', 'Placement', 'Date', 'Advertiser', 'Ad_Type', 'Ad_Status', 'Campaign_Start_Date', 'Campaign_End_Date']
        page_size = 100000
        src_col_name = 'Campaign'
        is_pln_df = True
        add_name_cl = True
        
        if not google_marketing_response.empty:
            postgre_write_main(google_marketing_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)
            do_drop = False
            out_str = ('Success')
            print(out_str)
            log_string(log_pltfrm, out_str)
try:
    log_pltfrm = 'google_marketing_platform'
    gmp_prep(log_pltfrm)
except KeyError as error:
    out_str = ('Key Error')
    print(out_str)
    log_string(log_pltfrm, out_str)
    print(error)
    log_string(log_pltfrm, error)