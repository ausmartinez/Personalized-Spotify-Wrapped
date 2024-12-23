'''
This script grabs the last 50 played songs, cleans the data, then appends the
newest songs by looking at the latest `played_at` field saved to `dat.csv`.
Spotify API appears to only log songs that are completed in the player and not
skipped or only partially listened to. Will result in slight underreporting.
'''


import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import pandas as pd
import os.path
import datetime


# Needs to be full path for task scheduluer
logDir = './log.csv'
datDir = './dat.csv'
configDir = './config.json'


def saveTheData(data):
	datadf = pd.DataFrame.from_dict(data)
	if (not os.path.isfile(datDir)):
		datadf = datadf.sort_values('played_at', ascending=False)
		datadf.to_csv(datDir)
	else:
		# `index_col` removes the unnamed index column
		orig = pd.read_csv(datDir, index_col=0)
		pd.to_datetime(orig['played_at']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
		pd.to_datetime(datadf['played_at']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
		mostRecent = orig.sort_values(
				'played_at', ascending=False
				)['played_at'].iloc[0]
		pd.concat([datadf[datadf['played_at'] > mostRecent], orig],
			ignore_index=True).to_csv(datDir)
		test = pd.read_csv(datDir, index_col=0).sort_values('played_at')
		print(test[['name', 'played_at']])


def cleanTheData(results):
	# Get the fields we want
	songs = []
	for s in results:
		data = s['track']
		data['played_at'] = s['played_at']
		# ******* album section *******
		# We just want the namese and ids for each artist on the album
		for i in data['album']['artists']:
			del i['href']
			del i['external_urls']
			del i['type']
			del i['uri']
		del data['album']['external_urls']
		del data['album']['href']
		del data['album']['available_markets']
		del data['album']['uri']
		# Get the largest image possible
		tempImage = ''
		max = 0
		for i in data['album']['images']:
			if (i['height'] > max):
				max = i['height']
				tempImage = i['url']
		data['album']['image'] = tempImage
		del data['album']['images']

		# ******* artists section *******
		for i in data['artists']:
			del i['external_urls']
			del i['type']
			del i['uri']

		# ******* other *******
		del data['available_markets']
		del data['is_local']
		del data['type']
		del data['uri']
		del data['href']
		del data['external_urls']
		del data['external_ids']
		del data['preview_url']
		songs.append(data)
	saveTheData(songs)


def writeToLog(message):
	now = datetime.datetime.now()
	if (not os.path.isfile(logDir)):
		temp = pd.DataFrame({'time': [now], 'message': [message]})
		temp.to_csv(logDir)
	else:
		log = pd.read_csv(logDir, index_col=0)
		pd.concat([log, pd.DataFrame({'time': [now], 'message': [message]})], 
			ignore_index=True).to_csv(logDir)


def run():
	try:
		with open(configDir, 'r') as f:
			config = json.load(f)

			sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
				client_id=config['client'],
				client_secret=config['secret'],
				redirect_uri='http://localhost:3000',
				scope='user-read-recently-played',
                open_browser=False,))

			results = sp.current_user_recently_played(limit=50)
			results = results['items']
			cleanTheData(results)
			writeToLog('SUCCESS: ' + str(pd.read_csv(datDir).shape[0]) 
			  + ' rows')
	except Exception as e:
		print(e, e.args)
		writeToLog('ERROR: ' + str(e))


if __name__ == '__main__':
	run()
