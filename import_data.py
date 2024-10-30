import pandas as pd # type: ignore
from sqlalchemy import create_engine # type: ignore
from datetime import datetime

try:
     df = pd.read_csv('D:\dw\Data after ETL/olist_orderS_dataset.csv')
     num_columns = df.shape[1]
     print(f"Số cột của DataFrame: {num_columns}")
#for col in shipping_limit_date:
#  df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %H:%M')


     engine = create_engine('mysql://root:Duong2003@localhost/oltp')

     df.to_sql('ORDERS', con=engine, if_exists='replace', index=False)

     print("Dữ liệu đã được import vào cơ sở dữ liệu MySQL.")
except Exception as e:
    print(f"Một ngoại lệ đã xảy ra: {e}")  