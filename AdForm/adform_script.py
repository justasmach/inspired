#!/usr/bin/env python
# coding: utf-8

import sys, os, os.path
base, tail = os.path.split(os.getcwd())
sys.path.append(base)
from helper_functions import *

#--Oauth Authentication--
def connect(payload_tkn, postman_tkn):
    url_tkn = "https://id.adform.com/sts/connect/token"
    header_auth = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Cache-Control': "no-cache",
        'Postman-Token': postman_tkn
        }

    auth_rsp = requests.request("POST", url_tkn, data=payload_tkn, headers=header_auth)
    
    oauth = json.loads(auth_rsp.text)
    auth_tkn = oauth['access_token']
    auth_tkn = 'Bearer ' + auth_tkn
    return auth_tkn

def chunk_split(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def adform_api(df_conf_req, payload_tkn, period_lst, log_pltfrm, postman_tkn, call_cnt, account, do_drop):
    
    # create lists for metrics and dimensions
    dim_lst = []
    met_lst = []
    
    t_name = 'adform_' + account + '_new'
    pk_name = 'adf' + account + '_new_pk'
    pk_lst = ['date', 'campaignID', 'lineItemID', 'orderID', 'bannerType']
    page_size = 100000
    src_col_name = 'campaign'
    is_pln_df = True
    
    

    for index, req in df_conf_req.iterrows():
        if not 'nan' in str(req['dimensions']) and not str(req['dimensions']) in pk_lst:
            dim_lst.append(req['dimensions'])
        if not 'nan' in str(req['metrics']):
            met_lst.append(req['metrics'])
            
    auth_tkn = connect(payload_tkn, postman_tkn)
    
    dim_chunk = (list(chunk_split(dim_lst, 3)))
    met_chunk = (list(chunk_split(met_lst, 10)))
    if len(met_chunk) > len(dim_chunk):
        batch_cnt = len(met_chunk)
    else:
        batch_cnt = len(dim_chunk)   
    
    df_response = pd.DataFrame()
    for i in range(0, batch_cnt):
        dims = []
        mets = []
        
        print(period_lst)
        log_string(log_pltfrm, period_lst)
        
        if i < len(dim_chunk):
            dims = dim_chunk[i]
        if i < len(met_chunk):
            mets = met_chunk[i]
        else:
            mets = ['conversions']
        dim_comb = pk_lst + dims
        period = json.loads(period_lst)
        
        for i in range(len(mets)):
            if mets[i] in ('conversionType1', 'conversionType2', 'conversionType3'):
                mets[i] = dict({"metric":"conversions", "specs":{"conversionType":mets[i]}})
            elif mets[i] in ('landingPages', 'infoPages', 'signUpPages', 'homepage', 'productPage', 'thankYouPage', 'download'):
                mets[i] = dict({"metric":"conversionsSpecs", "specs":{"pageCategory":mets[i]}})
        
        rep_url = "https://api.adform.com/v1/reportingstats/agency/reportdata"
        req_body = json.dumps({
          "dimensions": dim_comb,
          "metrics": mets,
          "filter": {
            "date": period
          }, 
           "paging": {
              "page": 1,
              "pageSize": 3000
           }
        })
        json.loads(req_body)
        headers = {
            'content-type': "application/json",
            'authorization': auth_tkn,
            'cache-control': "no-cache",
            'postman-token': postman_tkn
            }
        
        url = "https://api.adform.com/v1/reportingstats/agency/reportdata"
        #--Make the API calls--
        try:
            rep_rsp = requests.request("POST", url, data=req_body, headers=headers)
            call_cnt = call_cnt + 1
            out_str = ('Call count: ' + str(call_cnt))
            print(out_str)
            log_string(log_pltfrm, out_str)

            data_json = json.loads(rep_rsp.text)
            columns = data_json['reportData']['columns']
            
            out_str = ('Response code: ' + str(rep_rsp))
            print(out_str)
            log_string(log_pltfrm, out_str)
            
            column_hdrs = []
            for i in range(len(columns)):
                if columns[i]['key'] =='conversions':
                    column_hdrs.append(columns[i]['specs']['conversionType'])
                elif columns[i]['key'] =='conversionsSpecs':
                    column_hdrs.append(columns[i]['specs']['pageCategory'])
                else:
                    column_hdrs.append(columns[i]['key'])

            data_rows = data_json['reportData']['rows']

            # define empty pandas dataframe
            df_res_part = pd.DataFrame(columns = column_hdrs, data = data_rows)

            log_string(log_pltfrm, out_str)
            if df_response.empty:
                df_response = df_res_part
                # else do a left join and combine the two
            elif not df_res_part.empty:
                df_response = pd.merge(df_response, df_res_part,  how='outer', on=['date', 'campaignID', 'lineItemID', 'orderID'], suffixes=('', '_y'))
                df_response.drop_duplicates(subset =pk_lst, keep = 'first', inplace = True)
                to_drop = [x for x in df_response if x.endswith('_y')]
                df_response.drop(to_drop, axis=1, inplace=True)
            row_count = len(df_response.index)
            out_str = ('Row count: ' + str(row_count))
            log_string(log_pltfrm, out_str)
            print(out_str)
        except Exception as error:
            if 'Response [4' in rep_rsp or 'Response [5' in rep_rsp:
                out_str = ('Bad request, error 400 or 500')
                print(out_str)
                log_string(log_pltfrm, out_str)
            if 'quotaLimitExceeded' in str(data_json):
                is_time_given = re.search('etry\s*in\s*(\d{1,3})', str(data_json))
                if is_time_given:
                    time_to_wait = int(re.findall('etry\s*in\s*(\d{1,3})', str(data_json))[0]) + 15
                    out_str = ('Call quota reached. Wait time given, waiting for ' + str(time_to_wait) + ' secs and calling again')
                    print(out_str)
                    log_string(log_pltfrm, out_str)
                    time.sleep(time_to_wait)
                    call_cnt = adform_api(df_conf_req, payload_tkn, period_lst, log_pltfrm, postman_tkn, call_cnt, account, do_drop)
                    break
                else:
                    time.sleep(60)
                    call_cnt = adform_api(df_conf_req, payload_tkn, period_lst, log_pltfrm, postman_tkn, call_cnt, account, do_drop)
                    out_str = ('Call quota reached. Wait time not given, waiting for 60 secs and calling again')
                    log_string(log_pltfrm, out_str)
                    break
            else:
                out_str = 'Unknown key error, exiting'
                print(out_str)  
                print(error)
                log_string(log_pltfrm, out_str)
                log_string(log_pltfrm, error)
                sys.exit(1)
            print(error)
            log_string(log_pltfrm, error)
        
    if not df_response.empty:
        df_response = df_response.loc[:,~df_response.columns.duplicated()]
        postgre_write_main(df_response, t_name, pk_name, pk_lst, do_drop, page_size, src_col_name, is_pln_df, log_pltfrm)
        do_drop = False
        out_str = ('Success')
        print(out_str)
        log_string(log_pltfrm, out_str)
    return call_cnt
            
def adform_prep(log_pltfrm, call_cnt):
    out_str = 'Starting...'
    print(out_str)
    log_string(log_pltfrm, out_str)
    do_drop = True
    try:
        # read configuration from excel
        df_conf_base = pd.read_excel('adform_conf_1.xlsx', sheet_name='base', header=0)
        df_conf_req = pd.read_excel('adform_conf_1.xlsx', sheet_name='parameters', header=0)
        def_period = df_conf_req.iat[0,2]
        per_format = json.dumps("{\"from\":\"x1\",\"to\":\"x2\"}")
        per_format = json.loads(per_format.replace("\"", '"'))
        def_period = upd_last_90(def_period, per_format)
        out_str = (def_period)
        print(out_str)
        log_string(log_pltfrm, out_str)
        def_intv = 5
        period_lst = period_split(dict(eval(def_period)), def_intv)
        per_dct_lst = []
        for idx in range(len(period_lst) - 1):
            start = period_lst[idx]
            end = (datetime.strptime(period_lst[idx + 1], "%Y-%m-%d") - timedelta(days = 1)).strftime("%Y-%m-%d")
            if start > end:
                period = {"from":f"{start}","to":f"{start}"}
            else:
                period = {"from":f"{start}","to":f"{end}"}
            per_dct_lst.append(period)
        
        if len(df_conf_base) == 0:
            raise KeyError('No base data provided (view_id)')
        if pd.isna(df_conf_req['dimensions'].iloc[0]):
            for index, row in df_conf_req.iterrows():
                if pd.isna(row['metrics']):
                    raise KeyError('One or more metrics missing')        
                if pd.isna(row['date_range']) and index < 2:
                    raise KeyError('No date range provided')

        log_string(log_pltfrm, period_lst)
    except(NameError, XLRDError, KeyError) as error:
        out_str = ('Error while reading configuration file(s)')
        print(out_str)
        log_string(log_pltfrm, out_str)
        print(error)
        log_string(log_pltfrm, error)
        sys.exit(1)      

    # iterate over accounts
    for index, row in df_conf_base.iterrows():
        try:
            payload_tkn = str(row['payload_tkn'])
            postman_tkn = str(row['postman_tkn'])
            account = str(row['account'])
            out_str = ('Account: ' + account)
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
        for per_dct in per_dct_lst:
            period_lst = json.dumps(per_dct)

            call_cnt = adform_api(df_conf_req, payload_tkn, period_lst, log_pltfrm, postman_tkn, call_cnt, account, do_drop)

try:
    log_pltfrm = 'adform'
    call_cnt = 0
    adform_prep(log_pltfrm, call_cnt)
except(KeyError) as error:
    out_str = ('Key Error')
    print(out_str)
    log_string(log_pltfrm, out_str)
    print(error)
    log_string(log_pltfrm, error)
