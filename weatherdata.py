import sqlalchemy
from sqlalchemy import create_engine, Date, Float, String,Integer
from sqlalchemy.sql import text
from urllib.parse import quote_plus
import pandas as pd
import io
import requests

engine = sqlalchemy.create_engine("mysql+pymysql://root:%s@34.141.168.80/growrain" % quote_plus("Jinsha@15535!"))
query = '''Select DISTINCT d.Latitude as Latitude, d.Longitude as Longitude ,
min(b.Date) AS StartDate,max(b.Date) AS EndDate
from SensorReadings b 
join SensorLocations c on b.SensorCode = c.SensorCode
join Locations d on c.LocId = d.LocId
GROUP BY d.Latitude,d.Longitude;'''

with engine.connect().execution_options(autocommit=True) as con:
    rs = con.execute(text(query))
df = pd.DataFrame(rs.fetchall())
df = df.reset_index()

df["StartDate"] = df["StartDate"].astype(str)
df["EndDate"] = df["EndDate"].astype(str)
df["StartDate"] = df['StartDate'].str.replace('-','')
df["EndDate"] = df['EndDate'].str.replace('-','')

for index, row in df.iterrows():
    url = "https://power.larc.nasa.gov/api/temporal/hourly/point?Time=LST&parameters=T2M,QV2M,PRECTOTCORR&community=RE&longitude="+str(row["Longitude"])+"&latitude="+str(row["Latitude"])+"&start="+str(row["StartDate"])+"&end="+str(row["EndDate"])+"&format=CSV"
    urlData = requests.get(url).content
    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')),skiprows=11)
    rawData['Date'] = rawData['YEAR'].astype(str) + '-' + rawData['MO'].astype(str) + '-'+ rawData['DY'].astype(str)
    rawData["Latitude"] = row["Latitude"]
    rawData["Longitude"] = row["Longitude"]
    rawData = rawData.rename(columns={'HR': 'Hour', 'T2M': 'AirTemperature', 'QV2M': 'Humidity', 'PRECTOTCORR': 'Percipitation'})
    rawData = rawData[['Latitude','Longitude','Date','Hour','AirTemperature','Humidity', 'Percipitation']]
    rawData.to_sql(name = 'WeatherData',con = engine,dtype={'Latitude':Float,'Longitude':Float,'Date':Date,'Hour':Integer,'AirTemperature':Float,'Humidity':Float,'Percipitation':Float},if_exists = 'append',index = False)

query2 = '''SELECT DISTINCT Date,Hour from WeatherData;'''
with engine.connect().execution_options(autocommit=True) as con:
    rs2 = con.execute(text(query2))
datetime_df = pd.DataFrame(rs2.fetchall())
datetime_df = datetime_df.reset_index()

dt = []
counter = 100
for index,row in datetime_df.iterrows():
    dt.append(
		    {
			'DateHourID' : 'DH00'+ str(counter),
			'Date': row['Date'],
            'Hour': row['Hour'],
		    }
	    )
    counter = counter +1

datetime_df = pd.DataFrame(dt)
datetime_df.to_sql(name = 'DateHour',con = engine,dtype = {'DateHourID':String(50), 'Date':Date, 'Hour':Integer}, if_exists = 'append',index = False)

