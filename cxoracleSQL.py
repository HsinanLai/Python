import cx_Oracle 

cx_Oracle.init_oracle_client(lib_dir=r"D:\oracle\instantclient_21_7")
connection = cx_Oracle.connect('tq58/tq58@192.168.0.41/TOPPROD',                                                     
                                encoding='UTF-8', nencoding='UTF-8')
                                
curr=connection.cursor()
 
sql = ''' select * from aba_file where aba01 like \'%-%2204%\' ''' # 輸入你要查找的資料表語法
#retData = curr.execute(sql)
#print(retData.fetchone())
#print(retData.fetchone())
#curr.close

curr=connection.cursor()
#retData = curr.execute(sql)

for cur in curr.execute(sql):
  print(cur)
print(curr.rowcount)
curr.close


#SQL Statement帶參數
#在執行select的時候，帶參數是很正常的需求。
#所以where條件上，需要可以依需求來取得資料。
#
#str_sql = "select column from yourTable where col1=:col_a and col2=:col_b"
#cursor.execute(str_sql, {col_a: 'your condition', col_b: 'your condition'})
#
#或者也可以將參數先用dict格式裝起來#
#
#str_sql = "select column from yourTable where col1=:col_a and col2=:col_b"
#dict_con = {'col_a': 'your condition', 'col_b': 'your condition'}
#cursor.execute(str_sql, dict_con)