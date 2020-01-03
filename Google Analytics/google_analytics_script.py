#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def google_analytics(df_conf_req, view_id, key_file_location, scopes, period_lst, log_pltfrm):
    
    
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
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    # define empty pandas dataframe
    df_response = pd.DataFrame()
    
    # create lists for metrics and dimensions
    dim_lst = []
    met_lst = []
    start_date = period_lst[0]
    end_date = period_lst[1]
    for index, req in df_conf_req.iterrows():
        if not 'nan' in str(req['dimensions']):
            dim_lst.append(dict({'name':req['dimensions']}))
        if not 'nan' in str(req['metrics']):
            met_lst.append(dict({'expression':req['metrics']}))  
    
    met_batches = list()
    
    # split metric list into batches, since with a single API call, a max of 10 metrics can be requested
    i = 0
    while True:
        met_batch = list()
        stop = 0
        for index, met in enumerate(met_lst):
            if len(met_batch) < 10 and i < len(met_lst):
                res = met_batch.append(met_lst[i])
                i=i+1
        if len(met_batch) != 0:
            met_batches.append(met_batch)
        if i == len(met_lst):
            x = True
            break 

    # create empty dataframe for response segment
    
    out_str = 'Calling Google Analytics API...'
    log_string(log_pltfrm, out_str)
    try:
        # iterate over metric batches
        for index, batch in enumerate(met_batches):
            
            # define request body
            body={
            'reportRequests':
            [{
                'viewId': view_id,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': batch,
                'dimensions': dim_lst,
                'pageSize': 100000,
                'includeEmptyRows': True}]}

            # make the call to Google Analytics API
            response = analytics.reports().batchGet(body=body).execute()

            df_res_part = pd.DataFrame()
            
            # deconstruct JSON response
            for report in response.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

                # iterate over rows
                for data_row in report.get('data', {}).get('rows', []):
                    dimensions = data_row.get('dimensions', [])
                    dateRangeValues = data_row.get('metrics', [])
                    var_dict = {}

                    #iterate over dimensions
                    for header, dimension in zip(dimensionHeaders, dimensions):
                        if header not in df_res_part:
                            df_res_part[header] = pd.Series()
                            df_res_part.astype({header: 'object'}).dtypes
                        var_dict.update({header : str(dimension)})

                    # iterate over metrics
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            if metricHeader.get('name') not in df_res_part.columns:
                                df_res_part[metricHeader.get('name')] = pd.Series()
                            var_dict.update({metricHeader.get('name') : value})
                    df_res_part = df_res_part.append(var_dict, ignore_index=True)
        
            # if iteration is first and main dataframe is empty assign current response segment
            if df_response.empty:
                df_response = df_res_part
            # else do a left join and combine the two
            elif not df_res_part.empty:
                df_response = pd.merge(df_response, df_res_part,  how='outer', on=['ga:campaign', 'ga:adcontent', 'ga:channelGrouping', 'ga:keyword', 'ga:date', 'ga:sourceMedium'], suffixes=('', '_y'))
                df_response.drop_duplicates(subset =pk_lst, keep = 'first', inplace = True)
                to_drop = [x for x in df_response if x.endswith('_y')]
                df_response.drop(to_drop, axis=1, inplace=True)
            row_count_part = len(df_res_part.index)
            row_count_full = len(df_response.index)
            out_str = ('Batch ' + str(index + 1))
            print(out_str)
            log_string(log_pltfrm, out_str)
            out_str = (str(row_count_full) + ' row(s) received')
            print(out_str)
            log_string(log_pltfrm, out_str)
        df_response['ga_viewid'] = view_id
        
    except(http.HttpError) as error:
        print('GA API error')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)  
        log_string(log_pltfrm, error)
        sys.exit(1)
            
    return df_response


def ga_prep(log_pltfrm):
    out_str = 'Starting...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    do_drop = False
    try:
        # read configuration from excel
        df_conf_base = pd.read_excel('google_analytics_conf_1.xlsx', sheet_name='base', header=0)
        df_conf_req = pd.read_excel('google_analytics_conf_1.xlsx', sheet_name='parameters', header=0)
        if len(df_conf_base) == 0:
            raise KeyError('No base data provided (view_id)')
        if pd.isna(df_conf_req['dimensions'].iloc[0]):
            for index, row in df_conf_req.iterrows():
                if pd.isna(row['metrics']):
                    raise KeyError('One or more metrics missing')        
                if pd.isna(row['date_range']) and index < 2:
                    raise KeyError('No date range provided')
        per_format = "lst"
        period_lst = []
        period_lst.append(str(df_conf_req.iat[0,2]))
        period_lst.append(str(df_conf_req.iat[1,2]))
        period_lst = upd_last_90(period_lst, per_format)
        
        print(period_lst)
        log_string(log_pltfrm, period_lst)
        key_file_location = 'client_secrets.json'
        scopes = ['https://www.googleapis.com/auth/analytics.readonly']
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
            view_id = str(int(row['view_id']))
            out_str = ('View ID: ' + view_id)
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
        google_analytics_response = google_analytics(df_conf_req, view_id, key_file_location, scopes, period_lst, log_pltfrm)

        t_name = 'google_analytics_new'
        pk_name = 'ga_new_pk'
        pk_lst = ['ga_viewID', 'ga_sourceMedium', 'ga_date', 'ga_campaign', 'ga_adcontent', 'ga_channelGrouping', 'ga_keyword']
        page_size = 100000
        src_col_name = 'ga_campaign'
        is_pln_df = True
        add_name_cl = True
        
        if not google_analytics_response.empty:
            df_response = df_response.loc[:,~df_response.columns.duplicated()]
            postgre_write_main(google_analytics_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)
            do_drop = False
            out_str = ('Success')
            print(out_str)
            log_string(log_pltfrm, out_str)
try:
    log_pltfrm = 'google_analytics'
    ga_prep(log_pltfrm)
except KeyError as error:
    out_str = ('Key Error')
    print(out_str)
    log_string(log_pltfrm, out_str)
    print(error)
    log_string(log_pltfrm, error)