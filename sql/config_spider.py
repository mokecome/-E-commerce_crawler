# -*- coding: utf-8 -*-
import datetime
import gzip

n_time = datetime.datetime.now()
year=str(n_time.year)
month=str(n_time.month).zfill(2)
cycle='month'
if cycle=='month':
    day='00'
else:
    day=str(n_time.day).zfill(2)
  
def get_last_month():
    today = datetime.datetime.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    last_month = last_month.strftime("%Y%m")
    return last_month+day

Source_list=['Seven','Carrefour','Pxgo','momo','Ikea','Trplus','UrMart','Organic','Uniqlo','Net','Watsons','Poya','Medfirst','Healthyliving','Decathlon','Etmall','Petpetgo','Eslite','Pchome']
raw_cate_format=['GET_TIME','SOURCE','TAG1', 'TAG2', 'TAG3','TAG4','TAG_URL','TAG_ID']
raw_url_format=['GET_TIME','TAG_ID','TAG_URL', 'TAG_ADD_URL', 'PROD_NUM','PAGE_NUM','PROD_URL']
raw_product_format=['GET_TIME','PROD_URL','PROD_NM','PRICE','PRICE_SALE','DESC_RAW','SPEC','DESC_SHORT','DESC_FULL','BRAND','VEND_NM']
combine_cud_format=['GET_TIME','SOURCE','TAG1', 'TAG2', 'TAG3','TAG4','PROD_URL','PROD_NM','PRICE','PRICE_SALE','DESC_RAW','SPEC','NO']


def re_crawl_url(df_U,df_P):
    A=df_U[raw_url_format[-1]].to_list()
    B=df_P[raw_url_format[-1]].to_list()
    diff_A = list(set(A).difference(set(B)))
    if len(diff_A)/len(A)>0.01:
        return True,diff_A
    else:
        return False,diff_A
    
def get_gzip_compressed(data):
    compressed_str =gzip.compress(data.encode('utf-8'),compresslevel=9)
    return compressed_str
def get_gzip_decompressed(data):
    data=eval(data)
    decompressed_str = gzip.decompress(data)
    return decompressed_str.decode('utf-8')
    
    
    
 
def Tag_Check(df_pre,df_now,column,rate,num):
    add_tag=list(set(df_now[column]).difference(set(df_pre[column])))
    sub_tag=list(set(df_pre[column]).difference(set(df_now[column])))
    tag_list=[n for n in list(set(df_now[column]).intersection(set(df_pre[column])))]
    check_tag=[]
    for tag in tag_list:
        change=(len(df_now[df_now[column]==tag])-len(df_pre[df_pre[column]==tag]))
        if (((len(df_now[df_now[column]==tag])-len(df_pre[df_pre[column]==tag]))/len(df_pre[df_pre[column]==tag]))>rate) and (change>num):
            check_tag.append(tag)
    print('變動的TAG',add_tag,sub_tag)
    return check_tag

config={  
        'Seven':['python seven_class.py','python seven_url.py','eBusiness;scrapy crawl seven_detail -a name=seven_scrapy_url.csv','python seven_combine.py'],
        'Carrefour':['python carrefour_class.py','python carrefour_url.py','carrefour_spider;scrapy crawl carrefour','python carrefour_combine.py'],
        'Pxgo':['python pxgo_class.py','python pxgo_url.py','python pxgo_combine.py'],
        'momo':['python momo_class.py','momo_Incremental;scrapy crawl momo_increase','python momo_combine.py'],
        'Ikea':['python ikea_class.py','python ikea_url.py','python ikea_combine.py'],
        'Trplus':['python trplus_class.py','python trplus_url.py','python trplus_combine.py'],
        'UrMart':['python urmart_class.py','python urmart_url.py','python urmart_combine.py'],
        'Uniqlo':['python uniqlo_class.py','python uniqlo_url.py','python uniqlo_combine.py'],
        'Watsons':['python watsons_class.py','python watsons_url.py','python watsons_combine.py'],
        'Net':['python net_class.py','python net_url.py','python net_combine.py'],
        'Poya':['python poya_class.py','python poya_url.py','python poya_combine.py'],
        'Medfirst':['python medfirst_class.py','python medfirst_url.py','python medfirst_combine.py'],
        'Healthyliving':['python healthyliving_class.py','python healthyliving_url.py','python healthyliving_combine.py'],
        'Organic':['python organic_class.py','python organic_url.py','python organic_combine.py'],
        'Decathlon':['python decathlon_class.py','python decathlon_url.py','python decathlon_combine.py'],  
        'Etmall':['python etmall_class.py','python etmall_url.py','python etmall_combine.py'], 
        'Petpetgo':['python petpetgo_class.py','python petpetgo_url.py','python petpetgo_combine.py'], 
        'Eslite':['python eslite_class.py','Eslite_spider;scrapy crawl Eslite_u','python eslite_combine.py']
        }


log_sql_type={
"Log_cate":{"table_columns":["ID","GET_TIME","SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"],"table_type":"ID NVARCHAR(40),GET_TIME NVARCHAR(20),SOURCE NVARCHAR(20),KIND_1 NVARCHAR(50),KIND_2 NVARCHAR(50),KIND_3 NVARCHAR(50),KIND_4 NVARCHAR(50)","subset":["ID","GET_TIME","SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"]},
"Log_no":{"table_columns":["ID","GET_TIME","SOURCE","PRICE","PROD_NM","DESCRIPTION","SPEC"],"table_type":"ID NVARCHAR(40),GET_TIME NVARCHAR(20),SOURCE NVARCHAR(20),PRICE NVARCHAR(20),PROD_NM NVARCHAR(100),DESCRIPTION NVARCHAR(2000),SPEC NVARCHAR(2000)","subset":["ID","GET_TIME","SOURCE","PRICE","PROD_NM","DESCRIPTION","SPEC"]},

#"Log_mapping":{"table_columns":["ID","GET_TIME","SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"],"table_type":"ID NVARCHAR(40),GET_TIME NVARCHAR(20),SOURCE NVARCHAR(20),KIND_1 NVARCHAR(50),KIND_2 NVARCHAR(50),KIND_3 NVARCHAR(50),KIND_4 NVARCHAR(50)","subset":["ID","GET_TIME","SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"]},
"Log_url":{"table_columns":["ID","GET_TIME","SOURCE","URL"],"table_type":"ID NVARCHAR(40),GET_TIME NVARCHAR(20),SOURCE NVARCHAR(20),URL NVARCHAR(800)","subset":["ID","GET_TIME","SOURCE","URL"]}
}



train_sql_type={
"cate":{"table_columns":["SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"],"table_type":"SOURCE NVARCHAR(20),Kind_1 NVARCHAR(50),Kind_2 NVARCHAR(50),Kind_3 NVARCHAR(50),Kind_4 NVARCHAR(50)","subset":["SOURCE","KIND_1","KIND_2","KIND_3","KIND_4"]},
"no":{"table_columns":["SOURCE","PRICE","PROD_NM","DESCRIPTION","SPEC","FM_CODE"],"table_type":"SOURCE NVARCHAR(20),PRICE NVARCHAR(20),PROD_NM NVARCHAR(100),FM_CODE NVARCHAR(500),DESCRIPTION NVARCHAR(2000),SPEC NVARCHAR(2000)","subset":["FM_CODE"]},

"mapping":{"table_columns":["SOURCE","KIND_1","KIND_2","KIND_3","KIND_4","FM_CODE"],"table_type":"SOURCE NVARCHAR(20),KIND_1 NVARCHAR(50),KIND_2 NVARCHAR(50),KIND_3 NVARCHAR(50),KIND_4 NVARCHAR(50),FM_CODE NVARCHAR(500)","subset":["SOURCE","KIND_1","KIND_2","KIND_3","KIND_4","FM_CODE"]},
"url":{"table_columns":["SOURCE","URL","FM_CODE"],"table_type":"SOURCE NVARCHAR(20),URL NVARCHAR(600),FM_CODE NVARCHAR(500)","subset":["FM_CODE"]}
}

