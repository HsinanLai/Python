import fmrest
fms = fmrest.Server('https://filemakerhost',user='username',password='password',database='dbname',layout='Misc PO List',api_version='v1')
fms.login()
record = fms.get_records()
#print(record)

for r in record:
    print (r.keys())
    print (r.values())
    #print(str)
record1 = record[1]
print (record1)
print("--------------------------")
xx= record1.values()[0]
print (xx)
print(record.count())