import sched
import time
from datetime import datetime
from datetime import timedelta
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

## def RequestWaterLevelData(sc):
def RequestWaterLevelData(FromTime,ToTime):
	import requests
	import numpy as np

	Delta = np.ceil((ToTime - FromTime).total_seconds()/60)
	count = Delta/5 				# Requesting 5 minutes of data at a time

	df = pd.DataFrame()
	i = 0

	while i <= count:
		IntvFromTime = FromTime + timedelta(minutes=i*5)
		if (i == count):
			IntvToTime = ToTime
		else:
			IntvToTime = FromTime + timedelta(minutes=(i+1)*5)
		IntvFromTime = IntvFromTime.isoformat()[:-3]+'Z'
		IntvToTime = IntvToTime.isoformat()[:-3]+'Z'
		api_url = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/5cebf1e23d0f4a073c4bc0f6/data?time-series-code=wlo&from="+\
			IntvFromTime+"&to="+IntvToTime
		Persist = True
		j = 0
		while Persist and (j<=3):
			response = requests.get(api_url, timeout=5)
			j+=1
			if (response.status_code == 200):
				Persist = False
		json_payload = response.json()
		i += 1
		if len(json_payload) > 0:
			Current_df = json_normalize(json_payload)
			df = pd.concat([df, Current_df], ignore_index=True)

	df = df.loc[:, ['eventDate', 'value']]
	df.columns = ['time', 'value']
	return df

def DeviationFromBaseLine(dfbaseline, dfcurrentWaterLevel):
	LYWaterLevel = dfbaseline['value'].mean()
	CurrentWaterLevel = dfcurrentWaterLevel['value'].mean()
	return ((CurrentWaterLevel-LYWaterLevel)*100/LYWaterLevel)


## s = sched.scheduler(time.time, time.sleep)
## s.enter(0, 1, RequestWaterLevelData, (s,))
## s.run()

NumberofAlarms = 0
for i in range(0,10):
	now = datetime.utcnow()
	CurrentFrom = now - timedelta(minutes=20)
	CurrentTo = now
	LYFrom = CurrentFrom - timedelta(days=365)
	LYTo = now - timedelta(days=365)

	StdBaseLineDF = RequestWaterLevelData(LYFrom, LYTo)
	RawWaterLevelData = RequestWaterLevelData(CurrentFrom, CurrentTo)
	DegreeofDeviation = DeviationFromBaseLine(StdBaseLineDF, RawWaterLevelData)
	print("Current Water Level Deviation from Last Year: ", DegreeofDeviation)
	time.sleep(60)