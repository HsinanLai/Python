import cx_Oracle 
import pandas as pd
cx_Oracle.init_oracle_client(lib_dir=r"D:\oracle\instantclient_21_7")
connection = cx_Oracle.connect('user/password@192.168.0.41/TOPPROD',                                                     
                                encoding='UTF-8', nencoding='UTF-8') 
 
sql = '''select * from aba_file where 1=1''' # 輸入你要查找的資料表語法  
df = pd.read_sql(sql, con=connection) # 將資料表存成DataFrame格式
print(df) # 可以開始使用df做事囉


