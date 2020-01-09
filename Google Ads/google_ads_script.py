#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def google_ads(client, customer_id, page_size, df_conf_req, period, log_pltfrm):
    ga_service = client.get_service('GoogleAdsService', version='v2')
    channel_types = client.get_type('AdvertisingChannelTypeEnum')
    
    # create a list containing dimensions
    dim_str = ''
    dim_lst = []
    for index, row in df_conf_req.iterrows():
        dim_lst.append(row['dimensions'])
        dim_str = dim_str + row['dimensions'] + ', '
    dim_str = dim_str[:-2]
    
    out_str = 'Calling Google Ads API...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    # make the call to Google Ads using compiled parameters 
    print(period)
    log_string(log_pltfrm, period)
    query = f"SELECT {dim_str} FROM campaign WHERE {period}"

    response = ga_service.search(customer_id, query, page_size=page_size)

    df_response = pd.DataFrame()
    row_count = 0
    
    # iterate over response and add everything to a pandas dataframe
    try:
        for index, row in enumerate(response):
            row_count = index
            new_dim_lst = []
            new_dim_dtype_lst = []
            
            
            # create a list for dimension data types
            for dim in dim_lst:
                if 'int' in str(type(eval('row.' + dim))).lower():
                    new_dim_dtype_lst.append('int')
                elif 'double' in str(type(eval('row.' + dim))).lower():
                    new_dim_dtype_lst.append('float')
                else:
                    new_dim_dtype_lst.append('string')
                
                if 'google' in str(type(eval('row.' + dim))):
                    new_dim_lst.append('row.' + dim + '.value')
                else:
                    new_dim_lst.append('row.' + dim)
            
            # iterate over dimensions of a single row
            for idx, new_dim in enumerate(new_dim_lst):
                if dim_lst[idx] == 'campaign.advertising_channel_type':
                    df_response.loc[index, dim_lst[idx]] = str(channel_types.AdvertisingChannelType.Name(eval('row.' + dim_lst[idx])))
                    df_response[dim_lst[idx]] = df_response[dim_lst[idx]].astype('object')
                elif dim_lst[idx] == 'metrics.cost_micros':                 
                    df_response.loc[index, dim_lst[idx]] = float(float(eval(new_dim)) / 10000000)
                    df_response[dim_lst[idx]] = df_response[dim_lst[idx]].astype('float64')
                elif new_dim_dtype_lst[idx] == 'int':
                    df_response.loc[index, dim_lst[idx]] = int(eval(new_dim))
                    df_response[dim_lst[idx]] = df_response[dim_lst[idx]].astype('int64')
                elif new_dim_dtype_lst[idx] == 'float':
                    df_response.loc[index, dim_lst[idx]] = float(eval(new_dim))
                    df_response[dim_lst[idx]] = df_response[dim_lst[idx]].astype('float64')
                else:
                    df_response.loc[index, dim_lst[idx]] = str(eval(new_dim))
                    df_response[dim_lst[idx]] = df_response[dim_lst[idx]].astype('object')
                
    # return error messages if exception
    except google.ads.google_ads.errors.GoogleAdsException as ex:
        out_str = ('Request with ID "%s" failed with status "%s" and includes the '
              'following errors:' % (ex.request_id, ex.error.code().name))
        print(out_str)
        log_string(log_pltfrm, out_str)
        for error in ex.failure.errors:
            out_str = ('\tError with message "%s".' % error.message)
            print(out_str)
            log_string(log_pltfrm, out_str)
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    out_str = ('\t\tOn field: %s' % field_path_element.field_name)
                    print(out_str)
                    log_string(log_pltfrm, out_str)
        if "The customer can't be used because it isn't enabled" in error.message:
            out_str = "USER NOT ENABLED, CONTINUING"
            print(out_str)
            log_string(log_pltfrm, out_str)
        else:
            sys.exit(1)
    out_str = (str(row_count + 1) + ' row(s) received')
    print(out_str)
    log_string(log_pltfrm, out_str)
    return df_response
                     
def start():
    # If the google-ads.yaml file is present in home dir, GoogleAdsClient will read the configuration.  
    _DEFAULT_PAGE_SIZE = 500
    log_pltfrm = 'google_ads'
    out_str = ('Starting...')
    print(out_str)
    log_string(log_pltfrm, out_str)
    do_drop = False
    try:
        # read configuration from excel
        google_ads_client = (google.ads.google_ads.client.GoogleAdsClient.load_from_storage())
        df_conf_base = pd.read_excel('google_ads_conf_1.xlsx', sheet_name='base', header=0)
        df_conf_req = pd.read_excel('google_ads_conf_1.xlsx', sheet_name='parameters', header=0)
        if pd.isna(df_conf_base['customer_id'].iloc[0]):
            raise KeyError('No base data provided (customer_id(s))')
        if pd.isna(df_conf_req['period'].iloc[0]):
            raise KeyError('Period is missing')    
        for index, row in df_conf_req.iterrows():
            if pd.isna(row['dimensions']):
                raise KeyError('One or more dimensions missing')
        period = str(df_conf_req.iat[0,1])
        per_format = "segments.date >= 'x1' AND segments.date <= 'x2'"
        period = upd_last_90(period, per_format)
    except(NameError, XLRDError, KeyError, FileNotFoundError) as error:
        out_str = ('Error while reading configuration file(s)')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)
    
    # iterate over customers
    for index, row in df_conf_base.iterrows():              
        try:
            customer_id = str(int(row['customer_id']))
            #customer_id = str(df_conf_base.iat[0,0])
            out_str = ('Customer ID: ' + customer_id)
            print(out_str)
            log_string(log_pltfrm, out_str)
        except(KeyError) as error:
            out_str = ('Could not read column')
            print(out_str)
            log_string(log_pltfrm, out_str)
            print(error)
            log_string(log_pltfrm, error)
            sys.exit(1)
        
        # call defined methods
        
        out_str = 'Using multithreading!'
        print(out_str)
        log_string(log_pltfrm, out_str)        
        try_cnt = 0
        df_response = pd.DataFrame()
        while try_cnt < 5 and df_response.empty:
            try:
                try_cnt = try_cnt + 1
                df_response = func_timeout(1200, google_ads, args=(google_ads_client, customer_id, _DEFAULT_PAGE_SIZE, df_conf_req, period, log_pltfrm))
            except FunctionTimedOut:
                df_response = pd.DataFrame()
                out_str = ('Could not complete call within 20 mins. Attempt no. ' + str(try_cnt) + ' out of 5')
                print(out_str)
                log_string(log_pltfrm, out_str)
                
        t_name = 'google_ads_new'
        pk_name = 'google_ads_new_pk'
        pk_lst = ['campaign_id', 'segments_date']
        page_size = 100000
        src_col_name = 'campaign_name'
        is_pln_df = True
        
        print(df_response)
        
        if not df_response.empty:
            postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)
            do_drop = False
            out_str = ('Dataframe not empty')
            print(out_str)
            log_string(log_pltfrm, out_str)
        else:
            out_str = ('Dataframe empty')
            print(out_str)
            log_string(log_pltfrm, out_str)
    return df_response
        
try:
    start()
except(KeyError) as error:
    err_txt = 'A key error occurred:'
    print(err_txt)
    log_string(log_pltfrm, err_txt)
    print(error)
    log_string(log_pltfrm, error)