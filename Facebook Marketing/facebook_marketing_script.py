#!/usr/bin/env python
# coding: utf-8

# In[12]:


import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

def facebook_marketing_api(account_id, df_conf_req, access_token, period): 
    
    # create a list containing dimensions
    breakdown_lst_call = []
    dim_lst_call = []
    for index, row in df_conf_req.iterrows():
        if row['dimensions'] in ('publisher_platform', 'platform_position', 'action_attribution_windows'):
            breakdown_lst_call.append(row['dimensions']) 
        else:
            dim_lst_call.append(row['dimensions'])
            
    param_set_lst = {
        'time_range': f"{period}",
        'level': 'adset',
        'filtering': [],
        'action_attribution_windows': "['1d_view', '7d_view', '28d_view', '1d_click', '7d_click', '28d_click', 'default']",
        'breakdowns': f"{breakdown_lst_call}",
        'time_increment': {1}
    }
    
    print('Calling Facebook Marketing API...')
    FacebookAdsApi.init(access_token=access_token, api_version = 'v5.0')
    response = AdAccount(account_id).get_insights(fields = dim_lst_call, params = param_set_lst)
    df_response = pd.DataFrame()
    df_response_action = pd.DataFrame()
    row_count = 0

    var_lst = []
    var_lst_action = []

    for index, row in enumerate(response):
        row_count = index
        row_dict = vars(row)['_data']
        var_dict_core = {}
        for key in row_dict:
            if key in ('account_id', 'campaign_id', 'adset_id', 'date_start', 'objective', 'publisher_platform', 'platform_position', 'campaign_name'):
                var_dict_core.update({key : row_dict[key]})
        var_dict = {}
        var_dict_action = {}
        for key in row_dict:
            if key == 'action_values':
                action_values = row_dict['action_values']
                if action_values:
                    for key in action_values:
                        var_dict_action.update(key)
            elif key != 'date_stop':
                var_dict.update({key : row_dict[key]})
        var_lst.append(var_dict)
        if 'action_values' in str(row_dict.keys()):
            if row_dict['action_values']:
                var_dict_action.update(var_dict_core)
                var_lst_action.append(var_dict_action)
    if var_lst:
        df_response = df_response.append(var_lst, ignore_index=True)
    if var_lst_action:
        df_response_action = df_response_action.append(var_lst_action, ignore_index=True)
    print(str(row_count + 1) + ' row(s) received')

    return df_response, df_response_action

def facebook_marketing_prep(def_intv, account_id, good_run, try_count):
    do_drop = False
    do_drop_conv = False
    try:
        print('Starting...')

        try:
            # read configuration from excel
            df_conf_req = pd.read_excel('facebook_marketing_conf_1.xlsx', sheet_name='parameters', header=0)
            def_period = df_conf_req.iat[0,1]
            per_format = "{'since':'x1','until':'x2'}"
            def_period = upd_last_90(def_period, per_format)
            
            #db_config(filename = 'database.ini', section='postgresql')
            if pd.isna(df_conf_req['period'].iloc[0]):
                raise KeyError('Period is missing')    
            for index, row in df_conf_req.iterrows():
                if pd.isna(row['dimensions']):
                    raise KeyError('One or more dimensions missing')        
        except(NameError, XLRDError, KeyError) as error:
            print('Error while reading configuration file(s)')
            print(error)
            sys.exit(1)

        with open("fb_secrets.yaml", 'r') as secrets:
            try:
                secrets = yaml.safe_load(secrets)
                app_id = str(secrets['app_id'])
                app_secret = str(secrets['app_secret'])
                access_token = str(secrets['access_token'])
            except yaml.YAMLError as error:
                print('Could not read FB secrets')
                print(error)
                sys.exit(1)
        print(def_period)
        period_lst = period_split(dict(eval(def_period)), def_intv)
        per_dct_lst = []
        for idx in range(len(period_lst) - 1):
            start = period_lst[idx]
            end = (datetime.strptime(period_lst[idx + 1], "%Y-%m-%d") - timedelta(days = 1)).strftime("%Y-%m-%d")
            if start > end:
                period = {'since':f'{start}', 'until':f'{start}'}
            else:
                period = {'since':f'{start}', 'until':f'{end}'}
            per_dct_lst.append(period)
        # iterate over customers            
        try:
            print('Account ID: ' + account_id)
        except(KeyError) as error:
            print('Could not read column')
            print(error)
            sys.exit(1)
        for per_dct in per_dct_lst:
            if per_dct not in good_run:
                # call defined methods
                df_response = pd.DataFrame()
                df_response_action = pd.DataFrame()
                period = per_dct
                print("Period")
                print(period)
                facebook_marketing_resp = facebook_marketing_api(account_id, df_conf_req, access_token, period)
                df_response = facebook_marketing_resp[0]
                df_response_action = facebook_marketing_resp[1]

                t_name = 'facebook_marketing_new'
                pk_name = 'fb_new_pk'
                pk_lst = ['account_id', 'campaign_id', 'adset_id', 'date_start', 'objective', 'publisher_platform', 'platform_position']

                page_size = 1000
                src_col_name = 'campaign_name'
                is_pln_df = True

                if not df_response_action.empty:
                    postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df)
                    do_drop = False
                t_name = 'facebook_marketing_conv_new'
                pk_name = 'fb_cnv_new_pk'
                src_col_name = 'campaign_name'
                is_pln_df = True
                if not df_response_action.empty:
                    postgre_write_main(df_response_action, t_name, pk_name, pk_lst, do_drop_conv, page_size, src_col_name, is_pln_df)
                    do_drop_conv = False
                print('Success')
                good_run.append(period)
            

        return df_response, df_response_action
    except(KeyError) as error:
        print('Key error')
        print(error)
        sys.exit(1)
    except(NameError) as error:
        print('Name Error')
        print(error)
        sys.exit(1)
    except(FacebookRequestError) as error:
        if "Please reduce the amount of data you're asking for" in str(error):
            def_intv = def_intv - 1
            facebook_marketing_prep(def_intv, account_id, good_run)
            if def_intv < 2:
                print('day chunk size too large')
                print(error)
                sys.exit(1)
        elif 'There have been too many calls from this ad-account' in str(error) and try_count < 7:
            print('Too many calls, sleep for half hour, then try again')
            try_count = try_count + 1
            time.sleep(3600)
            facebook_marketing_prep(def_intv, account_id, good_run, try_count)       
        else:           
            print('Facebook marketing API Error')
            print(error)
            sys.exit(1)
            
df_conf_base = pd.read_excel('facebook_marketing_conf_1.xlsx', sheet_name='base', header=0)
def_intv = 3
try_count = 0
for index, row in df_conf_base.iterrows():  
    try:
        account_id = str(row['account_id'])
        #account_id = str(df_conf_base.iat[0,0])
        if pd.isna(df_conf_base['account_id'].iloc[0]):
            raise KeyError('No base data provided (account_id(s))')
        good_run = []
        start = facebook_marketing_prep(def_intv, account_id, good_run, try_count)
        df_response = start[0]
        df_response_action = start[1]
    except(KeyError) as error:
        print(error)
        sys.exit(1)

