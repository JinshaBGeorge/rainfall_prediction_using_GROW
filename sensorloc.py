import sqlalchemy
from sqlalchemy import (create_engine, Float, String)
from urllib.parse import quote_plus
import pandas as pd
import uuid

from google.cloud import storage
import io
from io import BytesIO

from geopy.geocoders import GoogleV3

config = "/home/jinsha_jb/msc-project-376215-269dfc44322f.json"
storage_client = storage.Client.from_service_account_json(config)
bucket_file = storage_client.get_bucket("growtimeseries")
file = bucket_file.blob("GrowLocations.csv")
data = file.download_as_string()
df = pd.read_csv(io.BytesIO(data),
		 encoding = "utf-8", sep = ",")

df1 = df.drop(columns=['Serial','SensorType','Type'], axis =1)
df1 = df1.rename(columns={'Latitude': 'Longitude', 
			  'Longitude': 'Latitude','Code':'SensorCode'})
df1 = df1[['SensorCode','Latitude','Longitude','BeginTime','EndTime']]
df1 = df1.drop_duplicates()

df2 = df[['Serial','SensorType','Type','Code']]
df2 = df2.drop_duplicates()

temp_df = df1[['Latitude','Longitude']]
temp_df = temp_df.drop_duplicates()

gmaps = GoogleV3(api_key='AIzaSyADjCYQCUf_rSsb-kEvWp89s3iYIrUlbU4')

latlng = []
Location = "null"

for index,row in temp_df.iterrows():
	if row['Latitude'] >= -90 and row['Latitude'] <= 90:
		loc = gmaps.reverse((row['Latitude'], row['Longitude']))
		if loc:
			Location = loc.address
	latlng.append(
		{
			'LocId' : 'LT' + str(uuid.uuid4().int)[:10],
			'Latitude': row['Latitude'],
            'Longitude': row['Longitude'],
			'Location': Location
		}
	)

df3 = pd.DataFrame(latlng)

sensor_loc_df = pd.merge(df1,df3,how ='left',
			  left_on = ['Latitude','Longitude'],
			    right_on = ['Latitude','Longitude'])
sensor_loc_df = sensor_loc_df.drop(columns=['Latitude',
					    'Longitude','Location'], axis =1)

print(sensor_loc_df.head())




engine = sqlalchemy.create_engine("mysql+pymysql://root:%s@34.141.168.80/growrain" 
				  % quote_plus("Jinsha@15535!"))

sensor_loc_df.to_sql(name = 'SensorLocations',con = engine,dtype = 
		     {'SensorCode':String(255),'BeginTime':String(255), 'EndTime':String(255),
				'LocId':String(50)}, if_exists = 'replace',index = False)
df3.to_sql(name = 'Locations',con = engine,dtype=
	   {'LocId':String(50),'Latitude':Float,'Longitude':Float,
     	'Location':String(255)},if_exists = 'replace',index = False)
df2.to_sql(name = 'SensorInfo', con = engine,dtype = 
	   {'Serial':String(5000),'SensorType':String(255),'Type':String(255),
     	'Code':String(255)},if_exists = 'replace', index = False)

with engine.connect() as con:
	con.execute('ALTER TABLE SensorLocations ADD PRIMARY KEY (SensorCode);')
	con.execute('ALTER TABLE Locations ADD PRIMARY KEY (LocId);')
	con.execute('ALTER TABLE SensorInfo ADD CONSTRAINT fk_SensorInfo FOREIGN KEY(Code) REFERENCES SensorLocations(SensorCode);')
	con.execute('ALTER TABLE SensorLocations ADD CONSTRAINT fk_latlng FOREIGN KEY(LocId) REFERENCES Locations(LocId);')
