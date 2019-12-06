#!/usr/bin/env python
# coding: utf-8

# In[6]:


import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def google_ads(client, customer_id, page_size, df_conf_req, period):
    ga_service = client.get_service('GoogleAdsService', version='v2')
    channel_types = client.get_type('AdvertisingChannelTypeEnum')
    
    # create a list containing dimensions
    dim_str = ''
    dim_lst = []
    for index, row in df_conf_req.iterrows():
        dim_lst.append(row['dimensions'])
        dim_str = dim_str + row['dimensions'] + ', '
    dim_str = dim_str[:-2]
    
    print('Calling Google Ads API...')
    # make the call to Google Ads using compiled parameters 
    print(period)
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
                    df_response.loc[index, dim_lst[idx]] = float(float(eval(new_dim)) / 100000)
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
        print('Request with ID "%s" failed with status "%s" and includes the '
              'following errors:' % (ex.request_id, ex.error.code().name))
        for error in ex.failure.errors:
            print('\tError with message "%s".' % error.message)
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print('\t\tOn field: %s' % field_path_element.field_name)
        sys.exit(1)
    print(str(row_count + 1) + ' row(s) received')
    return df_response
                     
def start():
    # If the google-ads.yaml file is present in home dir, GoogleAdsClient will read the configuration.  
    _DEFAULT_PAGE_SIZE = 500
    print('Starting...')
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
    except(NameError, XLRDError, KeyError) as error:
        print('Error while reading configuration file(s)')
        print(error)
        sys.exit(1)
    
    # iterate over customers
    for index, row in df_conf_base.iterrows():              
        try:
            customer_id = str(int(row['customer_id']))
            #customer_id = str(df_conf_base.iat[0,0])
            print('Customer ID: ' + customer_id)
        except(KeyError) as error:
            print('Could not read column')
            print(error)
            sys.exit(1)
        
        # call defined methods
        df_response = google_ads(google_ads_client, customer_id, _DEFAULT_PAGE_SIZE, df_conf_req, period)
    
        t_name = 'google_ads_new'
        pk_name = 'google_ads_new_pk'
        pk_lst = ['campaign_id', 'segments_date']
        page_size = 100000
        src_col_name = 'campaign_name'
        is_pln_df = True
        
        if not df_response.empty:
            postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df)
            do_drop = False
        print('Success')
    return df_response
        
try:
    start()
except(KeyError) as error:
    err_txt = 'A key error occurred:'
    print(err_txt)
    print('')
    print(error)
    compose_mail(err_txt, err_txt)
    send_mail()

