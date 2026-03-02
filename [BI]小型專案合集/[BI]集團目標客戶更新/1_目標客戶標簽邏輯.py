
import os
import sys
import time
from pathlib import Path
import datetime as dt

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

pd.options.mode.chained_assignment = None

load_dotenv(r"C:\Users\TW0002.TPTWKD\Desktop\Projects\Loren\code_resource\.env")
os.chdir(r"C:\Users\TW0002.TPTWKD\Desktop\Projects\Loren\有效成交客戶統計")

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd




########################### CRM Setting ###########################
###################################################################
########## Account Info ##########
# userID = os.getenv("CRM_USERID")
# pwd = os.getenv("CRM_PWD")
# security_token_TWOS = os.getenv("CRM_security_token_TWOS")
# security_token_CN = os.getenv("CRM_security_token_CN")
import sys, io
if getattr(sys.stdout, "buffer", None) is not None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if getattr(sys.stderr, "buffer", None) is not None:
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import os
from pathlib import Path

UNC_ROOT = r"\\192.168.1.218\KD共用"

def to_unc(path_str: str) -> str:
    """把以 Z: 開頭的路徑穩定轉成 UNC；其餘維持原樣"""
    if not path_str:
        return path_str
    p = path_str.strip()
    p = p.replace("/", "\\")
    if p[:2].lower() == "z:":
        # 去掉 "Z:" 再接到 UNC
        sub = p[2:].lstrip("\\/")
        return UNC_ROOT + "\\" + sub
    return p

def ensure_exists(path_str: str, must_exist=True):
    p = to_unc(path_str)
    if must_exist and not Path(p).exists():
        raise FileNotFoundError(f"檔案不存在：{p}")
    return p



region_order = ["TW","CN","SG","MY","HK","PH","ID","VN","IN","US","TH","AU","KR","JP"]
label_order = ["經營","開發中","開發","沉默"]
os_regions = [r for r in region_order if r not in ("TW","CN")]
placeholders = ", ".join(f"'{r}'" for r in os_regions)


userID = os.getenv("CRM_USERID_p10")
pwd = os.getenv("CRM_PWD_p10")
userID_cn = os.getenv("CRM_USERID_cn")
pwd_cn = os.getenv("CRM_PWD_cn")
security_token_TWOS = os.getenv("CRM_security_token_TWOS")
security_token_CN = os.getenv("CRM_security_token_CN")

########## TWOS ##########
url_token_TWOS = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
payload = {
  "grant_type" : "password",
  "client_id" : os.getenv("CRM_client_id_TWOS"),
  "client_secret" : os.getenv("CRM_client_secret_TWOS"),
  "username" : userID,
  "password" : pwd + security_token_TWOS
}

response = requests.post(url_token_TWOS, data=payload)
content = response.json()
ac_token_TWOS = content["access_token"]

## Header
header_TWOS = {"Authorization" : "Bearer " + ac_token_TWOS,
             "Content-Type" : "application/x-www-form-urlencoded"}
header_insert_TWOS = {"Authorization" : "Bearer " + ac_token_TWOS,
                    "Content-Type" : "application/json"}


########## CN ##########
url_token_CN = "https://login.xiaoshouyi.com/auc/oauth2/token"
# payload = {
#   "grant_type" : "password",
#   "client_id" : os.getenv("CRM_client_id_CN"),
#   "client_secret" : os.getenv("CRM_client_secret_CN"),
#   "username" : userID,
#   "password" : pwd + security_token_CN
# }

payload = {
  "grant_type" : "password",
  "client_id" : os.getenv("CRM_client_id_CN"),
  "client_secret" : os.getenv("CRM_client_secret_CN"),
  "username" : userID_cn,
  "password" : pwd_cn + security_token_CN
}

response = requests.post(url_token_CN, data=payload)
content = response.json()
ac_token_CN = content["access_token"]

## Header
header_CN = {"Authorization" : "Bearer " + ac_token_CN,
             "Content-Type" : "application/x-www-form-urlencoded"}
header_insert_CN = {"Authorization" : "Bearer " + ac_token_CN,
                    "Content-Type" : "application/json"}


########################## CRM Function ###########################
###################################################################
########## Datetime Function ##########
def fn_datetime(ts):
    try:
        if pd.isna(ts) or ts == '':
            return pd.NaT
        ts = float(ts)
        if ts > 1e12:
            ts = ts / 1000
        if ts < 0 or ts > 32503680000: 
            return pd.NaT
        return dt.datetime.fromtimestamp(ts)
    except Exception:
        return pd.NaT



########## EntityType Function ##########
url_descp_TWOS = f"https://api-p10.xiaoshouyi.com/rest/data/v2.0/xobjects/"
url_descp_CN = f"https://api-scrm.xiaoshouyi.com/rest/data/v2.0/xobjects/"

def entitype_CRM(field, url_select = url_descp_TWOS, header = header_TWOS):
    url_description = url_select + f'{field}/busiType'
    response = requests.get(url_description, headers=header)
    crm = response.json()
    return pd.DataFrame(crm['data']['records'])

def entitype_CRM_CN(field):
    return entitype_CRM(field, url_select = url_descp_CN, header = header_CN)

########## Query Function ##########
url_select_TWOS = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
url_select_CN = "https://api-scrm.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"

def query_CRM(select_query, url_select = url_select_TWOS, header = header_TWOS):
    old_qloc = ''
    scrm_data = pd.DataFrame()
    while True:
        data = {
            "xoql": select_query,
            "batchCount": 2000,
            "queryLocator": old_qloc
        }
        response = requests.post(url_select, headers=header, data=data)
        crm = response.json()
        data = pd.DataFrame(crm["data"]["records"])
        scrm_data = pd.concat([scrm_data, data], ignore_index=True, sort=False)
        
        if not crm['queryLocator']:
            break
        old_qloc = crm['queryLocator']
    return pd.DataFrame(scrm_data)


def query_CRM_CN(select_query):
    return query_CRM(select_query, url_select = url_select_CN, header = header_CN)


        
########## Date Range ##########
date_base = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
# date_base = dt.datetime(2025, 12, 1).replace(hour=0, minute=0, second=0, microsecond=0)
timestamp_ms = int(date_base.timestamp() * 1000)
# date_base = dt.datetime.strptime(sys.argv[1], "%Y/%m/%d")
start_date = date_base-relativedelta(years=3)
start_date_1 = date_base-relativedelta(years=1)
start_date_6m = start_date_1+relativedelta(months=6)

############### Query from SQL Server ###############
#####################################################
########## SQL Server Setting ##########
## raw data
engine_raw = create_engine(f"mssql+pyodbc:///?odbc_connect=raw_data", fast_executemany=True)

## clean data
engine_clean = create_engine(f"mssql+pyodbc:///?odbc_connect=clean_data", fast_executemany=True)

# ########## 訂單重點人物設計師 ##########
# sap_sales_data_query = f'''SELECT buyer, taxfree_basecurr, designer FROM dbo.sap_sales_data_processed
# WHERE planned_shipping_date >= '{start_date.strftime("%Y/%m/%d")}'  and planned_shipping_date < '{date_base.strftime("%Y/%m/%d")}' '''
# sap_sales = pd.read_sql(sap_sales_data_query, engine_clean)

# positive_cust = sap_sales.groupby(["buyer"])["taxfree_basecurr"].sum().reset_index()
# positive_cust = positive_cust.loc[positive_cust['taxfree_basecurr'] > 0]
# operated_cust = sap_sales.loc[sap_sales['buyer'].isin(positive_cust['buyer'])]
# operated_design = operated_cust['designer'].drop_duplicates()
# operated_design = operated_design.loc[operated_design != "00000000"]


########## Company ##########
crm_account_tw = pd.read_sql(f'''SELECT * FROM dbo.crm_account_tw where created_date < {date_base.timestamp()*1000}  ''', engine_raw)
crm_account_os = pd.read_sql(f'''SELECT * FROM dbo.crm_account_os where created_date < {date_base.timestamp()*1000}  ''', engine_raw)
crm_account_cn = pd.read_sql(f'''SELECT * FROM dbo.crm_account_cn where created_date < {date_base.timestamp()*1000} ''', engine_raw)

crm_account_tw['created_date'] = crm_account_tw['created_date'].apply(lambda x: fn_datetime(x))
crm_account_tw['record_date'] = crm_account_tw['record_date'].apply(lambda x: fn_datetime(x))

crm_account_os['created_date'] = crm_account_os['created_date'].apply(lambda x: fn_datetime(x))
crm_account_os['record_date'] = crm_account_os['record_date'].apply(lambda x: fn_datetime(x))

crm_account_cn['created_date'] = crm_account_cn['created_date'].apply(lambda x: fn_datetime(x))
crm_account_cn['record_date'] = crm_account_cn['record_date'].apply(lambda x: fn_datetime(x))


################### Get from CRM ####################
#####################################################
########## Opportunity ##########
#### TWOS ####
field_name = "opportunity"
select_query = f'''
SELECT 
opportunityName,money
,accountCode__c
,customItem190__c
,customItem220__c
FROM {field_name} 
WHERE customItem190__c >= {start_date_6m.timestamp()*1000} 
and customItem190__c < {date_base.timestamp()*1000} 
'''
TWOS_leads_orig = query_CRM(select_query)
TWOS_leads = TWOS_leads_orig[:]
TWOS_leads = TWOS_leads.replace(r'^\s*$', np.nan, regex=True)
TWOS_leads["customItem190__c"] = TWOS_leads["customItem190__c"].apply(fn_datetime)
TWOS_leads["customItem220__c"] = TWOS_leads["customItem220__c"].str.get(0)
TWOS_leads["success"] = np.where(TWOS_leads["customItem220__c"] == "結案（成交）", True, False)

#### CN ####
field_name = "opportunity"
select_query = f'''
SELECT 
opportunityName,money
,accountCode__c
,oppStatus__c
,customItem186__c
FROM {field_name} 
WHERE customItem186__c >= {start_date_6m.timestamp()*1000} 
and customItem186__c < {date_base.timestamp()*1000} 
'''
CN_leads_orig = query_CRM_CN(select_query)
CN_leads = CN_leads_orig[:]
CN_leads = CN_leads.replace(r'^\s*$', np.nan, regex=True)
CN_leads["customItem186__c"] = CN_leads["customItem186__c"].apply(fn_datetime)
CN_leads["success"] = np.where(CN_leads["oppStatus__c"] == "结案（成交）", True, False)


########## Contact ##########
#### TWOS ####
field_name = "customEntity22__c"
select_query = f'''
SELECT 
name
,customItem8__c
,contactCode__c__c 聯絡人
,customItem98__c
,contactPhone__c__c 聯絡人手機號
,customItem24__c 關係狀態
FROM {field_name} '''
TWOS_contact_orig = query_CRM(select_query)
TWOS_contact = TWOS_contact_orig[:]
TWOS_contact['customItem98__c'] = TWOS_contact['customItem98__c'].astype(str).str.pad(8, "left", "0")
TWOS_contact.loc[TWOS_contact['customItem98__c'] == "00000000", "customItem98__c"] = ""
TWOS_contact = TWOS_contact.replace(r'^\s*$', np.nan, regex=True)

#### CN ####
field_name = "accContactRelation__c"
select_query = f'''
SELECT
name
,customItem11__c
,contactCode__c 聯絡人
,customItem38__c
,contactPhone__c 聯絡人手機號
,customItem25__c 關係狀態
FROM {field_name} '''
CN_contact_orig = query_CRM_CN(select_query)
CN_contact = CN_contact_orig[:]
CN_contact['customItem38__c'] = CN_contact['customItem38__c'].astype(str).str.pad(8, "left", "0")
CN_contact.loc[CN_contact['customItem38__c'] == "00000000", "customItem38__c"] = ""
CN_contact = CN_contact.replace(r'^\s*$', np.nan, regex=True)






########## Mr.K ##########
#### TWOS ####
field_name = "customEntity23__c"
busitype = entitype_CRM(field_name)
select_query = f'''
SELECT 
name
,customItem4__c
,customItem19__c 聯絡人
,customItem6__c.contactPhone__c__c 聯絡人手機號
,customItem6__c.customItem24__c 關係狀態
,customItem30__c 是否舉行
,customItem10__c time1
,customItem11__c time2
FROM {field_name} 
WHERE customItem10__c >= {start_date_1.timestamp()*1000} 
and customItem10__c < {date_base.timestamp()*1000} '''
TWOS_K_orig = query_CRM(select_query)
TWOS_K = TWOS_K_orig[:]
TWOS_K = TWOS_K.replace(r'^\s*$', np.nan, regex=True)
TWOS_K['是否舉行'] = TWOS_K['是否舉行'].str.get(0)
TWOS_K['time1'] = TWOS_K['time1'].apply(fn_datetime)
TWOS_K['time2'] = TWOS_K['time2'].apply(fn_datetime)
TWOS_K['time'] = (TWOS_K['time2'] - TWOS_K['time1']).dt.total_seconds()/60
cond1 = TWOS_K['是否舉行'] == "是"
cond2 = TWOS_K['time'] >= 8
TWOS_K_finish = TWOS_K.loc[cond1 & cond2]


twos_k = TWOS_K_finish.rename(columns={'聯絡人':'twos_聯絡人'})
twos_c = TWOS_contact.rename(columns={'聯絡人':'twos_聯絡人','關係狀態':'twos_relation'})
twos_k = twos_k[twos_k['twos_聯絡人'].notna()]
twos_c = twos_c[twos_c['twos_聯絡人'].notna() & twos_c['twos_relation'].astype(str).str.contains('在職|在职')]
twos_k['twos_聯絡人'] = twos_k['twos_聯絡人'].astype(str)
twos_c['twos_聯絡人'] = twos_c['twos_聯絡人'].astype(str)
twos_k_companies_attended = ( twos_c.loc[twos_c['twos_聯絡人'].isin(set(twos_k['twos_聯絡人'])), 'customItem8__c']
    .dropna() .drop_duplicates().reset_index(drop=True))





#### CN ####
field_name = "customEntity26__c"
busitype = entitype_CRM_CN(field_name)
select_query = f'''
SELECT 
name
,customItem4__c
,customItem29__c 聯絡人
,customItem6__c.contactPhone__c 聯絡人手機號
,customItem6__c.customItem25__c 關係狀態
,customItem40__c 是否舉行
,customItem14__c time1
,customItem15__c time2
FROM {field_name} 
WHERE customItem14__c >= {start_date_1.timestamp()*1000} 
and customItem14__c < {date_base.timestamp()*1000} '''
CN_K_orig = query_CRM_CN(select_query)
CN_K = CN_K_orig[:]
CN_K = CN_K.replace(r'^\s*$', np.nan, regex=True)
CN_K['是否舉行'] = CN_K['是否舉行'].str.get(0)
CN_K['time1'] = CN_K['time1'].apply(fn_datetime)
CN_K['time2'] = CN_K['time2'].apply(fn_datetime)
CN_K['time'] = (CN_K['time2'] - CN_K['time1']).dt.total_seconds()/60
cond1 = CN_K['是否舉行'] == "是"
cond2 = CN_K['time'] >= 8
CN_K_finish = CN_K.loc[cond1 & cond2]


cn_k = CN_K_finish.rename(columns={'聯絡人':'cn_聯絡人'})
cn_c = CN_contact.rename(columns={'聯絡人':'cn_聯絡人','關係狀態':'cn_relation'})
cn_k = cn_k[cn_k['cn_聯絡人'].notna()]
cn_c = cn_c[cn_c['cn_聯絡人'].notna() & cn_c['cn_relation'].astype(str).str.contains('在職|在职')]
cn_k['cn_聯絡人'] = cn_k['cn_聯絡人'].astype(str)
cn_c['cn_聯絡人'] = cn_c['cn_聯絡人'].astype(str)
cn_k_companies_attended = (cn_c.loc[ cn_c['cn_聯絡人'].isin(set(cn_k['cn_聯絡人'])), 'customItem11__c' ]
    .dropna() .drop_duplicates() .reset_index(drop=True))




######### Mr.K Contact ##########
#### TWOS ####
field_name = "customEntity24__c"
busitype = entitype_CRM(field_name)
select_query = f'''
SELECT 
name
,customItem13__c.accountCode__c companyID,customItem6__c 聯絡人
,customItem2__c.contactPhone__c__c 聯絡人手機號, customItem2__c.customItem24__c 關係狀態
,customItem8__c 是否上線,customItem31__c
,customItem34__c time1
,customItem35__c time2
FROM {field_name} 
where customItem31__c >= {start_date_1.timestamp()*1000}
and customItem31__c < {date_base.timestamp()*1000} 
'''
TWOS_K_cont_orig = query_CRM(select_query)
TWOS_K_cont = TWOS_K_cont_orig[:]
TWOS_K_cont = TWOS_K_cont.replace(r'^\s*$', np.nan, regex=True)
TWOS_K_cont['是否上線'] = TWOS_K_cont['是否上線'].str.get(0)
TWOS_K_cont['customItem31__c'] = TWOS_K_cont['customItem31__c'].apply(fn_datetime)
TWOS_K_cont['time1'] = TWOS_K_cont['time1'].apply(fn_datetime)
TWOS_K_cont['time2'] = TWOS_K_cont['time2'].apply(fn_datetime)
TWOS_K_cont['time'] = (TWOS_K_cont['time2'] - TWOS_K_cont['time1']).dt.total_seconds()/60
cond1 = TWOS_K_cont['是否上線'] == "是"
cond2 = TWOS_K_cont['time'] >= 8
TWOS_K_cont_finish = TWOS_K_cont.loc[cond1 & cond2]

twos_mrk = TWOS_K_cont_finish.rename(columns={'聯絡人':'twos_聯絡人'})
twos_c = TWOS_contact.rename(columns={'聯絡人':'twos_聯絡人','關係狀態':'twos_relation'})
twos_mrk = twos_mrk[twos_mrk['twos_聯絡人'].notna()]
twos_c = twos_c[twos_c['twos_聯絡人'].notna() & twos_c['twos_relation'].astype(str).str.contains('在職|在职')]
twos_mrk['twos_聯絡人'] = twos_mrk['twos_聯絡人'].astype(str)
twos_c['twos_聯絡人'] = twos_c['twos_聯絡人'].astype(str)

twos_mrk_companies_attended = (twos_c.loc[  twos_c['twos_聯絡人'].isin(set(twos_mrk['twos_聯絡人'])),   'customItem8__c' ]
 .dropna() .drop_duplicates() .reset_index(drop=True))




#### CN ####
field_name = "customEntity27__c"
busitype = entitype_CRM_CN(field_name)
select_query = f'''
SELECT 
name
,customItem16__c,customItem24__c,customItem6__c 聯絡人
,customItem2__c.contactPhone__c 聯絡人手機號,  customItem2__c.customItem25__c 關係狀態
,customItem30__c 是否上線
,customItem17__c time1
,customItem18__c time2
FROM {field_name} 
where customItem24__c >= {start_date_1.timestamp()*1000}
and customItem24__c < {date_base.timestamp()*1000}
'''
CN_K_cont_orig = query_CRM_CN(select_query)
CN_K_cont = CN_K_cont_orig[:]
CN_K_cont = CN_K_cont.replace(r'^\s*$', np.nan, regex=True)
CN_K_cont['是否上線'] = CN_K_cont['是否上線'].str.get(0)
CN_K_cont['customItem24__c'] = CN_K_cont['customItem24__c'].apply(fn_datetime)
CN_K_cont['time1'] = CN_K_cont['time1'].apply(fn_datetime)
CN_K_cont['time2'] = CN_K_cont['time2'].apply(fn_datetime)
CN_K_cont['time'] = (CN_K_cont['time2'] - CN_K_cont['time1']).dt.total_seconds()/60
cond1 = CN_K_cont['是否上線'] == "是"
cond2 = CN_K_cont['time'] >= 8
CN_K_cont_finish = CN_K_cont.loc[cond1 & cond2]

cn_mrk = CN_K_cont_finish.rename(columns={'聯絡人':'cn_聯絡人'})
cn_c = CN_contact.rename(columns={'聯絡人':'cn_聯絡人','關係狀態':'cn_relation'})
cn_mrk = cn_mrk[cn_mrk['cn_聯絡人'].notna()]
cn_c = cn_c[cn_c['cn_聯絡人'].notna() & cn_c['cn_relation'].astype(str).str.contains('在職|在职')]
cn_mrk['cn_聯絡人'] = cn_mrk['cn_聯絡人'].astype(str)
cn_c['cn_聯絡人'] = cn_c['cn_聯絡人'].astype(str)
cn_mrk_companies_attended = (cn_c.loc[ cn_c['cn_聯絡人'].isin(set(cn_mrk['cn_聯絡人'])),  'customItem11__c'  ]  
    .dropna()  .drop_duplicates() .reset_index(drop=True))




################################################################################# TrackingRecord ##########
#### TWOS ####
field_name = "customEntity15__c"
select_query = f'''
SELECT name
,accountCode__c
,customItem40__c
,customItem128__c
,customItem4__c
,customItem45__c
,customItem177__c
,customItem59__c 聯絡人
,customItem48__c.contactPhone__c__c 聯絡人手機號,  customItem48__c.customItem24__c 關係狀態
FROM {field_name}
WHERE  customItem40__c >= {start_date_1.timestamp()*1000}
and customItem40__c < {date_base.timestamp()*1000} 
'''
TWOS_TR_orig = query_CRM(select_query)
TWOS_TR = TWOS_TR_orig[:] 
TWOS_TR = TWOS_TR.replace(r'^\s*$', np.nan, regex=True)
TWOS_TR['customItem4__c'] = TWOS_TR['customItem4__c'].str.get(0)
TWOS_TR['customItem128__c'] = TWOS_TR['customItem128__c'].str.get(0)
TWOS_TR['customItem177__c'] = TWOS_TR['customItem177__c'].str.get(0)
TWOS_TR['customItem40__c'] = TWOS_TR['customItem40__c'].apply(fn_datetime)

#### CN ####
field_name = "customEntity15__c"
select_query = f'''
SELECT name
,accountCode__c
,customItem4__c
,customItem40__c
,customItem99__c
,customItem106__c
,customItem63__c 聯絡人
,customItem55__c.contactPhone__c 聯絡人手機號,  customItem55__c.customItem25__c 關係狀態
FROM {field_name}
WHERE customItem40__c >= {start_date_1.timestamp()*1000} 
and customItem40__c < {date_base.timestamp()*1000} 
'''
CN_TR_orig = query_CRM_CN(select_query)
CN_TR = CN_TR_orig[:]
CN_TR = CN_TR.replace(r'^\s*$', np.nan, regex=True)
CN_TR['customItem4__c'] = CN_TR['customItem4__c'].str.get(0)
CN_TR['customItem99__c'] = CN_TR['customItem99__c'].str.get(0)
CN_TR['customItem106__c'] = CN_TR['customItem106__c'].str.get(0)
CN_TR['customItem40__c'] = CN_TR['customItem40__c'].apply(fn_datetime)
CN_TR['jobID'] = CN_TR['customItem4__c'].str.split(" ").str[0]


########## 近一年送樣客戶 ##########
#### TWOS ####
field_name = "customEntity28__c"
select_query = f'''
SELECT name
,createDate__c
,catalogGiftSendRequest__c.number__c 快遞單號
,customItem48__c
,customItem30__c
FROM {field_name} 
WHERE customItem30__c = '樣板'
AND createDate__c >= {start_date_1.timestamp()*1000} 
and createDate__c < {date_base.timestamp()*1000} 
'''
TWOS_template_orig = query_CRM(select_query)
TWOS_template = TWOS_template_orig[:]
TWOS_template = TWOS_template.replace(r'^\s*$', np.nan, regex=True)
TWOS_template['customItem30__c'] = TWOS_template['customItem30__c'].str.get(0)
TWOS_template['createDate__c'] = TWOS_template['createDate__c'].apply(fn_datetime)
TWOS_template = TWOS_template.loc[~TWOS_template['快遞單號'].isna()]

#### CN ####
field_name = "catalogSendDetail__c"
select_query = f'''
SELECT name
,customItem30__c
,customItem46__c
,createDate__c
,catalogGiftSendRequest__c.accountCode__c accountCode__c
FROM {field_name}
WHERE createDate__c >= {start_date_1.timestamp()*1000} 
and createDate__c < {date_base.timestamp()*1000}
'''
CN_template_orig = query_CRM_CN(select_query)
CN_template = CN_template_orig[:]
CN_template = CN_template.replace(r'^\s*$', np.nan, regex=True)
CN_template['customItem30__c'] = CN_template['customItem30__c'].str.get(0)
CN_template['createDate__c'] = CN_template['createDate__c'].apply(fn_datetime)
cond1 = CN_template['customItem30__c'].isin(["木地板样板", "样板"])
cond2 = CN_template['customItem46__c'] == "否"
CN_template = CN_template.loc[cond1 & cond2]


################### External Data ###################
#####################################################
# ########## 業務主管指定名單 ##########
# pure_design_TW = pd.read_excel("外部資料/業務主管指定名單_已確認.xlsx")
# pure_design_OS = crm_account_os.loc[crm_account_os['cofullname'].str[0] == "#"]
# pure_design_CN = pd.read_excel("外部資料/大陆业务重点客户名单-2024.3.28.xlsx", sheet_name = "重点公司")
# pure_design_ALL = pd.concat([pure_design_TW['公司代號'], pure_design_OS['company_id'], pure_design_CN['公司代号']]).dropna()


########## 近三年服務費設計師-找到公司 ##########
fee_design_TW = pd.read_excel(ensure_exists(r"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽\設計師指定費_TW_OK.XLSX"))
# fee_design_TW['companyid'] = fee_design_TW['companyid'].astype("Int64").astype(str).str.pad(8, "left", "0")
fee_design_TW = fee_design_TW[pd.to_datetime(fee_design_TW["日期"], errors="coerce").between(start_date, date_base)]
fee_contact_TW = crm_account_tw.loc[crm_account_tw['sap_company_id'].isin(fee_design_TW['SAP客代']), "sap_company_id"].drop_duplicates()

fee_design_OS = pd.read_excel(ensure_exists(r"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽\設計師指定費_OS_OK.xlsx"))
# fee_design_OS['Company ID'] = fee_design_OS['Company ID'].astype("Int64").astype(str).str.pad(8, "left", "0")
fee_contact_OS = crm_account_os.loc[crm_account_os['company_id'].isin(fee_design_OS['Company ID']), "company_id"].drop_duplicates()



cn_path = ensure_exists(r"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽\大陆设计师服务费支付明细.xlsx")
fee_design_CN = pd.read_excel(cn_path, sheet_name="设计师服务费登记")
s = pd.to_datetime(fee_design_CN["付款日期"], errors="coerce")
num_mask = fee_design_CN["付款日期"].apply(lambda x: isinstance(x, (int, float)) and not pd.isna(x))
if num_mask.any():
    s.loc[num_mask] = pd.to_datetime(
        fee_design_CN.loc[num_mask, "付款日期"].astype(float),
        unit="D", origin="1899-12-30", errors="coerce" )
fee_design_CN["付款日期"] = s
fee_design_CN = fee_design_CN[fee_design_CN["付款日期"].between(start_date, date_base) & fee_design_CN["付款日期"].dt.year.between(2000, 2099)
    & fee_design_CN["服务费方式"].notna() & fee_design_CN["服务费方式"].astype(str).str.strip().ne("-")]
fee_design_CN = ( fee_design_CN.loc[fee_design_CN[["CRM公司代号", "付款日期"]].isna().sum(axis=1) != 2, "CRM公司代号"] .dropna() .\
                 str.split("/") .explode() .str.strip() .apply(lambda x: x if x.startswith("CAC") else "CAC" + x)  .reset_index(drop=True))
fee_contact_CN = CN_contact.loc[CN_contact['customItem11__c'].isin(fee_design_CN), "customItem11__c"].drop_duplicates()


## 大陸服務費
field_name = "customEntity53__c"
select_query = f'''
SELECT customItem50__c 
FROM {field_name}
WHERE customItem2__c >= {start_date.timestamp()*1000} 
and customItem2__c < {date_base.timestamp()*1000}
'''
fee_contact_CN_CRM = query_CRM_CN(select_query)
def norm_to_one_col(obj, prefer=None, colname="value"):
    import pandas as pd
    
    if obj is None:
        return pd.DataFrame({colname: []})
    if isinstance(obj, pd.Series):
        return obj.astype(str).to_frame(colname)
    if isinstance(obj, (list, tuple, set)):
        return pd.DataFrame({colname: map(str, obj)})
    if isinstance(obj, pd.DataFrame):
        if obj.empty or obj.shape[1] == 0:
            return pd.DataFrame({colname: []})
        col = prefer if (prefer and prefer in obj.columns) else obj.columns[0]
        return obj[col].astype(str).to_frame(colname)
    return pd.DataFrame({colname: [str(obj)]})

fee_contact_ALL = pd.concat([norm_to_one_col(fee_contact_TW),norm_to_one_col(fee_contact_OS),
    norm_to_one_col(fee_contact_CN),norm_to_one_col(fee_contact_CN_CRM),],ignore_index=True)


# crm_account_os.to_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Projects\Loren\有效成交客戶統計\外部資料\crm_account_os.xlsx", index=False)

################## 台灣目標客戶邏輯 ##################
#####################################################
# ########## 近三年項目成交 ##########
# TWOS_leads_success = TWOS_leads.loc[TWOS_leads['suc# crm_account_tw['近三年項目成交'] = np.where(crm_account_tw['company_id'].isin(TWOS_leads_success), True, False)cess'] == True, "accountCode__c"]


# ########## 訂單重點人物 ##########
# designer_data_TWOS = TWOS_contact.loc[TWOS_contact['contactCode__c__c'].isin(operated_design) | TWOS_contact['customItem98__c'].isin(operated_design)]
# crm_account_tw['訂單重點人物'] = np.where(crm_account_tw['company_id'].isin(designer_data_TWOS['customItem8__c']), True, False)

########## 服務費設計師 ##########
crm_account_tw['服務費設計師'] = np.where(crm_account_tw['sap_company_id'].isin(fee_contact_ALL['value']), True, False)

# ########## 業務主管指定名單 ##########
# crm_account_tw['業務主管指定名單'] = np.where(crm_account_tw['company_id'].isin(pure_design_ALL), True, False)


########## 近一年K大 ##########
K_ALL = pd.concat([twos_k_companies_attended,cn_k_companies_attended
                 , twos_mrk_companies_attended , cn_mrk_companies_attended
                  ])
crm_account_tw['近一年K大'] = np.where(crm_account_tw['company_id'].isin(K_ALL), True, False)


# ########## 近一年拜訪 ##########
# TWOS_visit = TWOS_TR.loc[TWOS_TR['customItem128__c'] == "A1 拜訪", "accountCode__c"]
# crm_account_tw['近一年拜訪'] = np.where(crm_account_tw['company_id'].isin(TWOS_visit), True, False)

########## 近一年拜訪 ##########
visit_聯絡人 = (TWOS_TR.loc[ TWOS_TR['customItem128__c'] == 'A1 拜訪', '聯絡人' ] .dropna().astype(str).unique())
twos_c_injob = TWOS_contact[TWOS_contact['聯絡人'].notna() & TWOS_contact['關係狀態'].astype(str).str.contains('在職|在职')].copy()
twos_c_injob['聯絡人'] = twos_c_injob['聯絡人'].astype(str)
TWOS_visit = ( twos_c_injob.loc[ twos_c_injob['聯絡人'].isin(visit_聯絡人), 'customItem8__c' ] .dropna().drop_duplicates().reset_index(drop=True))
crm_account_tw['近一年拜訪'] = np.where(crm_account_tw['company_id'].isin(TWOS_visit), True, False)



########## 近一年送樣 ##########
crm_account_tw['近一年送樣'] = np.where(crm_account_tw['company_id'].isin(TWOS_template['customItem48__c']), True, False)

########## 近半年有項目 ##########
TWOS_leads_6month = TWOS_leads.loc[TWOS_leads['customItem190__c'] >= start_date_6m, "accountCode__c"]
# TWOS_leads_6month = TWOS_leads["accountCode__c"]
crm_account_tw['近半年有項目'] = np.where(crm_account_tw['company_id'].isin(TWOS_leads_6month), True, False)

########## 近半年有詢價 ##########
TWOS_TR_quotation = TWOS_TR.loc[TWOS_TR["customItem40__c"] >= start_date_6m]
# TWOS_TR_quotation = TWOS_TR
cond1 = TWOS_TR_quotation['customItem4__c'].str.contains("C1", na=False)
cond2 = TWOS_TR_quotation['customItem45__c'].str.contains("詢價|報價|quotation")
TWOS_quotation = TWOS_TR_quotation.loc[cond1|cond2, "accountCode__c"]
crm_account_tw['近半年有詢價'] = np.where(crm_account_tw['company_id'].isin(TWOS_quotation), True, False)

########## 近半年聯繫不上 ##########
TWOS_TR_pick = TWOS_TR.loc[TWOS_TR["customItem40__c"] >= start_date_6m]
TWOS_TR_pick = TWOS_TR_pick[TWOS_TR_pick['關係狀態'].astype(str).str.contains('在職|在职')]

TWOS_TR_pick['類型'] = TWOS_TR_pick['customItem128__c'].str[:2]
cond1 = TWOS_TR_pick['類型'].isin(["A2", "B2", "D2"])
cond2 = TWOS_TR_pick['customItem177__c'] == "未接"
TWOS_TR_pick['valid'] = (~cond1 & ~cond2)


TWOS_pick_pivot = TWOS_TR_pick.groupby(['accountCode__c']).agg({'name': 'count', 'valid': 'sum'}).reset_index()
TWOS_pick_pivot.columns = ["accountCode__c", "all", "valid"]
TWOS_pick_pivot["invalid"] = TWOS_pick_pivot["all"] - TWOS_pick_pivot["valid"]
TWOS_pick_pivot = TWOS_pick_pivot.loc[TWOS_pick_pivot['invalid'] == TWOS_pick_pivot['all']]
crm_account_tw['近半年聯繫不上'] = np.where(crm_account_tw['company_id'].isin(TWOS_pick_pivot['accountCode__c']), True, False)
crm_account_tw.loc[crm_account_tw['created_date'] >= start_date_6m, "近半年聯繫不上"] = False



################## 海外目標客戶邏輯 ##################
#####################################################
# ########## 近三年項目成交 ##########
# crm_account_os['近三年項目成交'] = np.where(crm_account_os['company_id'].isin(TWOS_leads_success), True, False)

# ########## 訂單重點人物 ##########
# crm_account_os['訂單重點人物'] = np.where(crm_account_os['company_id'].isin(designer_data_TWOS['customItem8__c']), True, False)

########## 服務費設計師 ##########
crm_account_os['服務費設計師'] = np.where(crm_account_os['company_id'].isin(fee_contact_ALL['value']), True, False)

# ########## 業務主管指定名單 ##########
# crm_account_os['業務主管指定名單'] = np.where(crm_account_os['company_id'].isin(pure_design_ALL), True, False)

########## 近一年K大 ##########
crm_account_os['近一年K大'] = np.where(crm_account_os['company_id'].isin(K_ALL), True, False)

########## 近一年拜訪 ##########
crm_account_os['近一年拜訪'] = np.where(crm_account_os['company_id'].isin(TWOS_visit), True, False)

########## 近一年送樣 ##########
crm_account_os['近一年送樣'] = np.where(crm_account_os['company_id'].isin(TWOS_template['customItem48__c']), True, False)

########## 近半年有項目 ##########
crm_account_os['近半年有項目'] = np.where(crm_account_os['company_id'].isin(TWOS_leads_6month), True, False)

########## 近半年有詢價 ##########
crm_account_os['近半年有詢價'] = np.where(crm_account_os['company_id'].isin(TWOS_quotation), True, False)

########## 近半年聯繫不上 ##########
crm_account_os['近半年聯繫不上'] = np.where(crm_account_os['company_id'].isin(TWOS_pick_pivot['accountCode__c']), True, False)
crm_account_os.loc[crm_account_os['created_date'] >= start_date_6m, "近半年聯繫不上"] = False


################## 大陸目標客戶邏輯 ##################
#####################################################
# ########## 近三年項目成交 ##########
# CN_leads_success = CN_leads.loc[CN_leads['success'] == True, "accountCode__c"]
# crm_account_cn['近三年項目成交'] = np.where(crm_account_cn['company_id'].isin(CN_leads_success), True, False)

# ########## 訂單重點人物 ##########
# designer_data_CN = CN_contact.loc[CN_contact['contactCode__c'].isin(operated_design) | CN_contact['customItem38__c'].isin(operated_design)]
# crm_account_cn['訂單重點人物'] = np.where(crm_account_cn['company_id'].isin(designer_data_CN['customItem11__c']), True, False)

########## 服務費設計師 ##########
crm_account_cn['服務費設計師'] = np.where(crm_account_cn['company_id'].isin(fee_contact_ALL['value']), True, False)

# ########## 業務主管指定名單 ##########
# crm_account_cn['業務主管指定名單'] = np.where(crm_account_cn['company_id'].isin(pure_design_ALL), True, False)

########## 近一年K大 ##########
crm_account_cn['近一年K大'] = np.where(crm_account_cn['company_id'].isin(K_ALL), True, False)

########## 近一年拜訪 ##########
CN_visit = CN_TR.loc[CN_TR['jobID'].isin(["A1", "A3-2", "C3"]), "accountCode__c"]
crm_account_cn['近一年拜訪'] = np.where(crm_account_cn['company_id'].isin(CN_visit), True, False)

########## 近一年送樣 ##########
crm_account_cn['近一年送樣'] = np.where(crm_account_cn['company_id'].isin(CN_template['accountCode__c']), True, False)

########## 近半年有項目 ##########
CN_leads_6month = CN_leads.loc[CN_leads['customItem186__c'] >= start_date_6m, "accountCode__c"]
# CN_leads_6month = CN_leads["accountCode__c"]
crm_account_cn['近半年有項目'] = np.where(crm_account_cn['company_id'].isin(CN_leads_6month), True, False)

########## 近半年有詢價 ##########
CN_TR_quotation = CN_TR.loc[CN_TR["customItem40__c"] >= start_date_6m]
# CN_TR_quotation = CN_TR
CN_TR_quotation = CN_TR_quotation.loc[CN_TR_quotation['customItem99__c'] == "【项目】客户询价、报价", "accountCode__c"].drop_duplicates()
crm_account_cn['近半年有詢價'] = np.where(crm_account_cn['company_id'].isin(CN_TR_quotation), True, False)

########## 近半年聯繫不上 ##########
CN_TR_pick = CN_TR.loc[CN_TR["customItem40__c"] >= start_date_6m]
CN_TR_pick = CN_TR_pick[CN_TR_pick['關係狀態'].astype(str).str.contains('在職|在职')]

cond1 = CN_TR_pick['jobID'].isin(["A2", "B2", "D2"])
cond2 = CN_TR_pick['customItem106__c'] == "未接听"
CN_TR_pick['valid'] = np.where(~cond1&~cond2, True, False)

CN_pick_pivot = CN_TR_pick.groupby(['accountCode__c']).agg({'name': 'count', 'valid': 'sum'}).reset_index()
CN_pick_pivot.columns = ["accountCode__c", "all", "valid"]
CN_pick_pivot["invalid"] = CN_pick_pivot["all"] - CN_pick_pivot["valid"]
CN_pick_pivot = CN_pick_pivot.loc[CN_pick_pivot['invalid'] == CN_pick_pivot['all']]
crm_account_cn['近半年聯繫不上'] = np.where(crm_account_cn['company_id'].isin(CN_pick_pivot['accountCode__c']), True, False)
crm_account_cn.loc[crm_account_cn['created_date'] >= start_date_6m, "近半年聯繫不上"] = False


###################### OUTPUT #######################
#####################################################
crm_account = pd.concat([crm_account_tw, crm_account_os, crm_account_cn])
crm_account = crm_account[['company_id','服務費設計師',  '近一年K大', '近一年拜訪', '近一年送樣', '近半年有項目', '近半年有詢價', '近半年聯繫不上']]

crm_account[['服務費設計師', '近一年K大', '近一年拜訪', '近一年送樣', '近半年有項目', '近半年有詢價', '近半年聯繫不上']] =\
crm_account[['服務費設計師', '近一年K大', '近一年拜訪', '近一年送樣', '近半年有項目', '近半年有詢價', '近半年聯繫不上']].apply(lambda x: np.where(x, 1, 0))

# crm_account.to_excel("crm_account.xlsx", index=False)
crm_account_tw[['服務費設計師',  '近一年K大', '近一年拜訪', '近一年送樣', '近半年有項目', '近半年有詢價', '近半年聯繫不上']].apply(lambda x: x.value_counts())



total_account = pd.read_sql(f'''
                    SELECT 'CN' AS 地區, company_type, cofullname, sap_company_id SAP公司代號, [company_id] FROM [raw_data].[dbo].[crm_account_cn] 
                    WHERE created_date < '{date_base.timestamp()*1000}'
                --    AND (company_type LIKE '%C%' OR company_type LIKE '%D%') AND company_type NOT LIKE '%DP%'
                            union all
                    SELECT 'TW' AS 地區, company_type, cofullname, sap_company_id SAP公司代號, [company_id] FROM [raw_data].[dbo].[crm_account_tw] 
                    WHERE  created_date < '{date_base.timestamp()*1000}'
                --    AND (company_type LIKE '%C%' OR company_type LIKE '%D%') AND company_type NOT LIKE '%DP%'
                            union all
                    SELECT CASE WHEN data_region_name = '印度分公司' THEN 'IN'
                                WHEN data_region_name = 'Japan Branch' THEN 'JP'
                                WHEN LEFT(data_region_name, 2) IN ('CA','PD') THEN 'US'
                                WHEN CHARINDEX('-', data_region_name) > 0 
                                    THEN LEFT(data_region_name, CHARINDEX('-', data_region_name) - 1)
                                    ELSE data_region_name
                                END AS 地區, company_type, cofullname, sap_company_id SAP公司代號, [company_id] FROM [raw_data].[dbo].[crm_account_os]
                    WHERE  created_date < '{date_base.timestamp()*1000}'
                --    AND (company_type LIKE '%C%' OR company_type LIKE '%D%') AND company_type NOT LIKE '%DP%'      
                    ''', engine_raw)


company_map = pd.read_sql(f'''SELECT  company_id ,company_id_parent 關聯公司代號 , sap_company_id_parent
           FROM [raw_data].[dbo].[crm_related_company]''', engine_raw)

total_account = pd.merge(total_account,company_map , on = 'company_id', how = 'left')


# 拼接標籤
crm_account = pd.merge(total_account, crm_account, on='company_id', how='left')


# 近三年銷售金額
# 最近發貨日期  

cutover = dt.datetime(2025, 1, 1)
raw_start = start_date
raw_end   = min(date_base, cutover)            # raw 段： [start_date, min(date_base, 2025-01-01))
proc_start = max(start_date, cutover)          # proc 段： [max(start_date, 2025-01-01), date_base)
proc_end   = date_base
raw_start_s = raw_start.strftime("%Y-%m-%d")
raw_end_s   = raw_end.strftime("%Y-%m-%d")
proc_start_s = proc_start.strftime("%Y-%m-%d")
proc_end_s   = proc_end.strftime("%Y-%m-%d")

sql_union = f"""
WITH combined AS (
    -- 2025-01-01 之前，用 raw_data.final_sales_history
    SELECT 
        CAST(kunag AS NVARCHAR(50))      AS SAP公司代號,
        netwr_l    AS 未稅本位幣,
        CAST(wadat   AS DATE)            AS 發貨日期
    FROM [raw_data].[dbo].[final_sales_history]
    WHERE wadat >= '{raw_start_s}' AND wadat < '{raw_end_s}' and is_sales = 'V' and len(kunag) >0

    UNION ALL

    -- 2025-01-01（含）之後，用 clean_data.sap_sales_data_processed
    SELECT
        CAST(buyer AS NVARCHAR(50))            AS SAP公司代號,
        taxfree_basecurr AS 未稅本位幣,
        CAST(planned_shipping_date AS DATE)     AS 發貨日期
    FROM [clean_data].[dbo].[sap_sales_data_processed]
    WHERE planned_shipping_date >= '{proc_start_s}' 
      AND planned_shipping_date < '{proc_end_s}'
      AND is_count = 1
)
SELECT 
    SAP公司代號,
    SUM(未稅本位幣)      AS 近三年銷售金額,
    MAX(發貨日期)        AS 最近發貨日期
FROM combined
GROUP BY SAP公司代號
"""
sap_sales_data = pd.read_sql(sql_union, engine_clean)


crm_account = pd.merge(crm_account, sap_sales_data, on='SAP公司代號', how='left')
crm_account["近三年有銷售"] = np.where( crm_account["近三年銷售金額"] >= -99999999999, 1, 0)

# 貼標
conditions  = [
    (crm_account[["近三年有銷售", "服務費設計師"]].sum(axis=1) > 0),    # 經營
    (crm_account[["近一年K大", "近一年拜訪", "近一年送樣", "近半年有項目", "近半年有詢價"]].sum(axis=1) > 0),    # 開發中
    (crm_account[["近半年聯繫不上"]].sum(axis=1) > 0)]    # 沉默
choices = ["經營", "開發中", "沉默"]
crm_account["label"] = np.select(conditions , choices, default="開發")

crm_account["label"] = pd.Categorical( crm_account["label"], categories=label_order,ordered=True)
crm_account = crm_account.sort_values("label").reset_index(drop=True)







# 抓無效公司清單
invalid = pd.read_sql("""SELECT company_id, 剔除原因 FROM clean_data.dbo.crm_account_invalid""", engine_raw)

# 只取 剔除原因 包含 "管制" 的公司
invalid_ctrl = invalid[invalid["剔除原因"].astype(str).str.contains("管制", na=False)]
invalid_ctrl = pd.merge(invalid_ctrl,company_map , on = 'company_id', how = 'left')

# 從 total_account 剔除這些公司, 要從company_id 和 關聯公司中剔除











## 關聯公司數據
crm_account_related= crm_account[["地區", "關聯公司代號", "label"]].copy()
crm_account_related = (crm_account_related.drop_duplicates(subset=["關聯公司代號"], keep="first").reset_index(drop=True))





# 將關聯公司對應的標籤下放到子公司
merged_child = pd.merge(crm_account, crm_account_related[['關聯公司代號','label']], on="關聯公司代號", how="left")
# merged_child = merged_child[merged_child['company_type'].astype(str).str.contains(r'C|D', na=False)]
# merged_child = merged_child[~merged_child["company_id"].isin(invalid["company_id"])]
# merged_child = merged_child[~merged_child["company_id"].isin(invalid_ctrl["company_id"]) &
#                                   ~merged_child["關聯公司代號"].isin(invalid_ctrl["關聯公司代號"])]


###########################################  所有數據







import glob
def get_latest_excel(folder_path, keyword):
    file_pattern = os.path.join(folder_path, f"*{keyword}*.xlsx")
    files = glob.glob(file_pattern)
    files = [f for f in files if not os.path.basename(f).startswith("~$")]
    if files:
        latest_file = max(files, key=os.path.getctime)
        print(f"找到的最新文件: {os.path.basename(latest_file)}")  
        return latest_file
    else:
        print("沒有找到匹配的文件！")
        return None 

暫封存_file = r"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽"
暫封存_keyword = "暫封存"
暫封存_path = get_latest_excel(暫封存_file, 暫封存_keyword)
暫封存_df = pd.read_excel(暫封存_path,sheet_name= '暫封存3310')


account_tw_total = pd.read_sql(f'''SELECT * FROM [bi_ready].[dbo].[crm_tw_account_datail] ''', engine_raw)

suspended_company_set = set(  暫封存_df["company_id"].dropna() .astype(str))
def contains_flag(val, flag):
    if pd.isna(val):
        return False
    return flag in str(val)

def judge_account_validity(row, suspended_company_set):
    #  無效資料區域（bool）
    if row["invalid_region_flag"]:
        return "無效資料區域"
    # 倒閉
    if str(row["closed_flag"]).strip().upper() == "TRUE":
        return "倒閉"
    #  非主關聯（bool）
    if not row["is_main_related"]:
        return "非主關聯"
    #  管制（bool）
    if row["restricted_flag"]:
        return "管制"
    #  非 C/D 類
    if not row["cd_type"]:
        return "非C/D類"
    # 審批撤回未提交（不包含 Approved）
    approval_status = str(row["approval_status"]).lower()
    if "approved" not in approval_status:
        return "審批撤回未提交"
    # 暫封存（用 company_id ∈ 暫封存名單）
    company_id = str(row["company_id"])
    if company_id in suspended_company_set:
        return "暫封存"
    # 公司資料不全
    if ( not row["has_employee"]
        or str(row["all_contacts_left"]).strip().upper() == "TRUE"
        or str(row["all_contacts_invalid"]).strip().upper() == "TRUE"):
        return "公司資料不全"
    # 是否包含關鍵字（bool）
    if row["keyword_flag"]:
        return "包含關鍵字"
    # 公司地址無效（bool）
    if row["addr_short_nohao"]:
        return "公司地址無效"
    return "有效名單"

account_tw_total["名單狀態"] = account_tw_total.apply( lambda row: judge_account_validity(row, suspended_company_set),axis=1)

# 如果merged_child_final 的company_id 在 suspended_company_set 中 , label 改為 "暫封存"
# freeze_status = {"暫封存",'公司資料不全'}
freeze_status = {"暫封存"}
freeze_company_set = set( account_tw_total.loc[ account_tw_total["名單狀態"].isin(freeze_status), "company_id" ] .dropna() .astype(str))
merged_child["label_y"] = merged_child.apply(  lambda row: "暫封存" if str(row["company_id"]) in freeze_company_set else row["label_y"], axis=1)
# 審批撤回未提交
unsubmitted_company_set = set( account_tw_total.loc[ account_tw_total["名單狀態"]=='審批撤回未提交', "company_id" ] .dropna() .astype(str))

merged_child_copy = merged_child.copy()
merged_child_copy = merged_child_copy.rename(columns={"地區": "area"}).copy()

# merged_child.to_excel(fr"C:\Users\TW0002.TPTWKD\Desktop\Projects\Loren\有效成交客戶統計\crm_label_withP{date_base.strftime("%Y%m%d")}.xlsx", index=False)
merged_child_copy['query_time'] = date_base
kd.write_to_sql(df=merged_child_copy[['area','company_id','label_y','query_time']].rename(columns={"label_y":"label"}),db_name='bi_ready',table_name='target_account_tag',if_exists='replace')
time.sleep(30)
kd.write_to_sql(df=merged_child_copy,db_name='bi_ready',table_name='target_account_detail',if_exists='replace')



# (account_tw_total

########################################### BI 清整


merged_child_final = merged_child[["地區", "company_id","label_y"]].rename(columns={"label_y": "label"})
total_account = pd.read_sql(f'''
                    SELECT data_region_name, null as is_franchise, 'CN' AS 地區, company_type, sap_company_id, [company_id] FROM [raw_data].[dbo].[crm_account_cn] union all
                    SELECT data_region_name, is_franchise, 'TW' AS 地區, company_type, sap_company_id, [company_id] FROM [raw_data].[dbo].[crm_account_tw] union all
                    SELECT data_region_name, is_franchise, CASE WHEN data_region_name = '印度分公司' THEN 'IN'
                                WHEN data_region_name = 'Japan Branch' THEN 'JP'
                                WHEN LEFT(data_region_name, 2) IN ('CA','PD') THEN 'US'
                                WHEN CHARINDEX('-', data_region_name) > 0 
                                    THEN LEFT(data_region_name, CHARINDEX('-', data_region_name) - 1)
                                    ELSE data_region_name
                                END AS 地區, company_type, sap_company_id, [company_id] FROM [raw_data].[dbo].[crm_account_os]
                    WHERE  CASE WHEN data_region_name = '印度分公司' THEN 'IN'
                                WHEN data_region_name = 'Japan Branch' THEN 'JP'
                                WHEN LEFT(data_region_name, 2) IN ('CA','PD') THEN 'US'
                                WHEN CHARINDEX('-', data_region_name) > 0 
                                    THEN LEFT(data_region_name, CHARINDEX('-', data_region_name) - 1)
                                    ELSE data_region_name
                                    END IN ({placeholders})        
                    ''', engine_raw)

grouped_withchild =  pd.merge( total_account,merged_child_final[['company_id', 'label']], on='company_id', how='left')
# grouped_withchild.to_excel(fr"C:\Users\TW0002.TPTWKD\Desktop\Projects\Loren\有效成交客戶統計\crm_account_label_{date_base.strftime("%Y%m%d")}.xlsx", index=False)
grouped_withchild["label"].value_counts(dropna=False)

# 「加盟」的 label 設為真正空值
mask_franchise = (
    (grouped_withchild['is_franchise'].astype(str).str.contains('是', na=False)) |
    (grouped_withchild['data_region_name'].str.contains('加盟', na=False)) |
    (grouped_withchild['company_id'].astype(str).isin(unsubmitted_company_set)) # 未過審
)
grouped_withchild.loc[mask_franchise, 'label'] = np.nan

# 經營_df = pd.read_excel(暫封存_path,sheet_name= '資料效正中，先貼經營270')
# mask_經營 = (grouped_withchild['company_id'].astype(str).isin(set( 經營_df['公司代號'] )))
# grouped_withchild.loc[mask_經營, 'label'] = '經營'


# 移除 data_region_name 欄位
grouped_withchild = grouped_withchild.drop(columns=['data_region_name'])
grouped_withchild_df = grouped_withchild.rename(columns={"地區": "area"}).copy()
grouped_withchild_df['query_time'] = date_base
# write_to_sql(df=grouped_withchild_df,db_name='bi_ready',table_name='target_account_tag',if_exists='replace')





# 先取出 total_account 中「加盟」的 company_id
franchise_company_ids = total_account.loc[(total_account['is_franchise'].astype(str).str.contains('是', na=False)) 
| (total_account['data_region_name'].str.contains('加盟', na=False)), 'company_id'].unique()

valid_company_ids = set( account_tw_total.loc[ account_tw_total["名單狀態"].astype(str).str.contains("有效名單|公司資料不全"), "company_id"  ])

# 去重後統計
result = (
    merged_child
    .loc[
        (merged_child["company_id"] == merged_child["關聯公司代號"])
        & (~merged_child["關聯公司代號"].isin(franchise_company_ids))
        & (~merged_child["company_id"].isin(unsubmitted_company_set))
        & (   ((merged_child["地區"] == "TW")  & (merged_child["company_id"].isin(valid_company_ids)))
            | ((merged_child["地區"] != "TW")  & (merged_child["company_type"].astype(str).str.contains("C|D")  & (~merged_child["company_id"].isin(invalid['company_id']))))
        )]
    .drop_duplicates(["關聯公司代號"])
    .groupby(["地區", "label_y"])
    .size()
    .reset_index(name="計數")
    .rename(columns={"label_y": "label"})
)

# 建立完整組合，補齊缺失
full_index = pd.MultiIndex.from_product([region_order,label_order], names=['地區','label'])
result = result.set_index(['地區','label']).reindex(full_index, fill_value=0).reset_index()


# 為每個地區加上合計
result = pd.concat([ result,result.groupby('地區', as_index=False)['計數'].sum().assign(label="合計")], ignore_index=True)

# 排序（地區與label固定順序）
result['地區'] = pd.Categorical(result['地區'], categories=region_order, ordered=True)
result['label'] = pd.Categorical(result['label'], categories=label_order+["合計"], ordered=True)
result = result.sort_values(['地區','label']).reset_index(drop=True)

result_df = result.copy()
result_df = result_df.rename(columns={"地區":"region","label":"label","計數":"count"})
result_df['query_time'] = date_base
result_df['month'] = result_df['query_time'].dt.to_period('M').dt.to_timestamp()  # 當月 1 號



#######################################################################################################
kd.write_to_sql(df=result_df, db_name='bi_ready',
             table_name='target_account_count', if_exists='replace')

kd.write_to_sql(df=result_df, db_name='bi_ready',
             table_name='target_account_count_monthly',
             dedup_keys=['region','month','label'], keep='old', if_exists='update')
#######################################################################################################



# 固定格式導出到excel
def insert_blank_after_total(df: pd.DataFrame, blanks=4, total_label="合計", keep_region_blank=True):
    rows = []
    for _, r in df.iterrows():
        rows.append(r)
        if str(r.get("label")) == total_label:
            for _ in range(blanks):
                if keep_region_blank:
                    rows.append(pd.Series({"地區": "", "label": "", "計數": ""}))
                else:
                    rows.append(pd.Series({"地區": r["地區"], "label": "", "計數": ""}))
    return pd.DataFrame(rows, columns=df.columns).reset_index(drop=True)

result_formular = insert_blank_after_total(result, blanks=4, total_label="合計", keep_region_blank=True)
out_path = to_unc(fr"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽\目標客戶標簽統計\crm_account_summary_{date_base.strftime('%Y%m%d')}.xlsx")
result_formular.to_excel(out_path, index=False)
