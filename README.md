# -E-commerce_crawler
## 目的
   依據需求抓取市面上的電商Seven,Carrefour,Pxgo,momo,Ikea,Trplus,UrMart,Uniqlo,Watsons,Poya,Eslite,Pchome..  
   1.擴增分類的關鍵字  
   2.擴充模型的訓練資料以及後續retrain   
   3.電商商品價格監控         
   4.電商商品趨勢分析
## 環境需求
Python3.8
## 安裝相關套件
pip install requests
pip install Faker==19.2.0  
pip install parsel==1.6.0  
pip install nums-from-string==0.1.2  
pip install pandas==2.0.3  
pip install scikit-learn==1.3.2  
pip install scipy==1.10.1  
pip install selenium==4.12.0  
pip install scrapy==2.8.0  
## 使用方法
[爬蟲執行.pptx](https://github.com/mokecome/-E-commerce_crawler/files/14355109/default.pptx)  
python spider_execute.py

## 資料格式
| 欄位名稱 | 資料格式 | 中文說明 | 舉例 | 
| :----:|:----: | :----: | :----:|
| GET_TIME | str | 獲取時間 | 20240200 | 
| SOURCE | str | 資料來源 |Seven | 
| TAG1 | str | 類別1 |飲料沖泡 | 
| TAG2 | str | 類別2 |茶飲料 | 
| TAG3 | str | 類別3 |本周注目活動 | 
| TAG4 | str | 類別4 |TOP30排行 | 
| TAG_URL | str | 類別頁url |https://shop.7-11.com.tw/shop/rui004.faces?catid=65965| 
| TAG_ID | str | 呼叫api的id |1214 |
| TAG_ADD_URL | str | 類別頁翻頁的url |... |
| PROD_NUM | str | 該頁的商品項數 |58 |
| PAGE_NUM | str | 翻頁共有多頁數 |2 |
| PROD_URL | str | 商品名 | |
| PROD_NM | str | 呼叫api的id | |
| PRICE | str | 價格 | 500|
| PRICE_SALE | str | 折扣價 | 450|
| DESC_SHORT | str | 商品名稱旁的敘述 ||
| DESC_FULL | str | 商品特色 | |
| DESC_RAW | str | 商品詳情 | |
| SPEC | str | 規格 |"[]"|
| BRAND | str | 品牌 ||
| VEND_NM | str | 廠商名稱 | |
| NO | str | 商品編號 |563324|

[資料格式說明.xlsx](https://github.com/mokecome/-E-commerce_crawler/files/14354464/default.xlsx)

## 資料更新
每月定期更新與做報表檢視,其中Seven,Carrefour,momo,誠品數據量較大採用scrapy  
對某些電商監控價格 EX:momo採用增量式爬蟲更新  
[爬蟲品質報表.pptx](https://github.com/mokecome/-E-commerce_crawler/files/14355114/default.pptx)

## 流程圖
使用scrapy  
![scrapy](https://github.com/mokecome/-E-commerce_crawler/assets/75211039/7e171b2b-c5bf-4c37-80cf-161e35893f95)

未使用scrapy  
![request](https://github.com/mokecome/-E-commerce_crawler/assets/75211039/bcef34e9-21d8-4178-b611-df79fffd0c35)

