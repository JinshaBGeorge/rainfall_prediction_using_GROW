import sqlalchemy
from sqlalchemy import (create_engine, Float, String, Date,Integer)
from sqlalchemy.sql import text
from urllib.parse import quote_plus
import pandas as pd

from google.cloud import storage
import io
import uuid
from io import BytesIO
 
storage_client = storage.Client.from_service_account_json("/home/jinsha_jb/msc-project-376215-269dfc44322f.json")
bucket_file = storage_client.get_bucket("growtimeseries")

counter = 100
for i in range (0,193):
    file = bucket_file.blob("temp/GrowTimeSeries_"+str(i)+".csv")
    data = file.download_as_string()
    df = pd.read_csv(io.BytesIO(data),encoding = "utf-8", sep = ",")
    
    df1 = df[['Device','sTime','sReadingType','sActualValue']]
    df1[['Date','Time']] = df1['sTime'].str.split('T',expand = True)
    df1['Time'] = df1['Time'].str.replace(r'Z$','')
    df1['Hour'] = df1.Time.str[:2]
    df1 = df1.rename(columns={'Device':'SensorCode'})
    df1 = df1[['SensorCode','Date','Hour','Time','sReadingType','sActualValue']]
    df1 = df1.drop_duplicates()

    engine = sqlalchemy.create_engine("mysql+pymysql://root:%s@34.141.168.80/growrain"
                                       % quote_plus("Jinsha@15535!"))

    df1.to_sql(name = 'SensorReadings',con = engine,
               dtype = {'SensorCode':String(255),'Date':Date, 'Hour':Integer, 
                        'Time':String(255), 'sReadingType':String(255), 'sActualValue':Float}, 
               if_exists = 'append',index = False)
    
query = '''SELECT DISTINCT Date, Hour from SensorReadings;'''

with engine.connect().execution_options(autocommit=True) as con:
    rs = con.execute(text(query))
dh_df = pd.DataFrame(rs.fetchall())
dh_df = dh_df.reset_index()

dt = []
counter = 100
for index,row in dh_df.iterrows():
    dt.append(
		    {
			'DateHourID' : 'DH00'+ str(counter),
			'Date': row['Date'],
            'Hour': row['Hour'],
		    }
	    )
    counter = counter +1

datetime_df = pd.DataFrame(dt)
datetime_df.to_sql(name = 'DateHour',con = engine,
                   dtype = {'DateHourID':String(50), 'Date':Date, 'Hour':Integer}, 
                   if_exists = 'append',index = False)

with engine.connect() as con:
    con.execute('ALTER TABLE SensorReadings ADD CONSTRAINT fk_SensorCode FOREIGN KEY(SensorCode) REFERENCES SensorLocations(SensorCode);')
    con.execute('ALTER TABLE DateHour ADD PRIMARY KEY (DateHourID);')
    con.execute('ALTER TABLE SensorReadings ADD COLUMN DateHourID varchar(50);')
    con.execute('UPDATE SensorReadings A JOIN DateHour B ON A.Date = B.Date and A.Hour = B.Hour SET A.DateHourID = B.DateHourID;')
    con.execute('ALTER TABLE SensorReadings DROP COLUMN Date,DROP COLUMN Hour;')
    con.execute('ALTER TABLE SensorReadings ADD CONSTRAINT fk_DateHour FOREIGN KEY(DateHourID) REFERENCES DateHour(DateHourID);')
    con.execute('ALTER TABLE SensorReadings ADD PRIMARY KEY (SensorCode, sReadingType, DateHourID, Time);')




