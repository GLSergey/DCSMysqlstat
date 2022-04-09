import json
import pymysql
import datetime
import os
import glob
sample_data='E:\DCSScript'
#print(os.listdir(sample_data))
con=pymysql.connect(host='localhost',user='root',password='dcswordpassword',port=3306,db='test_bd')
for file in list(glob.glob(os.path.join(sample_data, '*.json'))):
    name_file=os.path.basename(file)
    cursor1 = con.cursor()
    cursor1.execute("select 1 as flag from dcsw_stat where name_file='" + name_file + "' Limit 1")
    myresult = cursor1.fetchone()
    cursor1.close()
    #print(myresult)
    #con.commit()
    if myresult !=(1,):
        fullfname = sample_data+'\\'+os.path.basename(file) #'E:\DCSScript\Kotel3_v8-_Mar_12_2022_at_17_22_44.json'
        fnames = fullfname.split("\\")
        fname = (fnames[len(fnames)-1])
        rfname = fname[::-1]
        rec_time = (rfname[5:7:1] + ':' + rfname[8:10:1] + ':' + rfname[11:13:1])[::-1]
        print(rec_time)  # 'OBRUT'
        rec_date = (rfname[17:21:1] + ' ' + rfname[22:24:1] + ' ' + rfname[25:28:1])[::-1]
        print(rec_date)
        missname=(rfname[30:])[::-1]
        print(missname)
        from dateutil import parser
        dt = parser.parse(rec_date + '  ' + rec_time)
        print(dt)
        cursor=con.cursor()
        with open(fullfname, 'r', encoding='utf-8') as f: #открыли файл
            root_data = json.load(f) #загнали все из файла в переменную
        for level_0 in root_data:
            drec_miss=level_0
            level_0_data = root_data[level_0]
            cursor.execute("insert into dcsw_stat (miss_name,miss_datetime,record_miss,id,lastJoin,times,names,name_file) values(%s,%s,%s,%s,%s,""%s"",""%s"",%s)",(missname,dt,drec_miss,level_0_data['id'],level_0_data['lastJoin'],json.dumps(level_0_data.get('times',{})),json.dumps(level_0_data.get('names',{})),name_file))
        con.commit()
con.close()

