import requests
import pandas as pd
import numpy as np
import streamlit as st

access_token = "BQAjOVAp1Foy3CNHpusXOS0aqD0Dn1fRHznKUzvIEY5mBSo_CkfBOPABMM3Xm3p0R68OAPfp5_4gHqkzdAPpi2oX_nXEM0aVOTr4Nn0OwyXCVTkXqB4"

# Spotify track ID of the song you want to get information about
artist_id = '6eUKZXaKkcviH0Ku9w2n3V'

artist_url = f'https://api.spotify.com/v1/artists/{artist_id}/albums'

headers = {'Authorization': f'Bearer {access_token}'}

response_artists = requests.get(artist_url, headers = headers)

alb_ids = np.array([])
track_ids = np.array([])
songs_per_album = np.array([])
if response_artists.status_code == 200:
    response_artists = response_artists.json()
    for item in response_artists['items']:
        alb_ids = np.append(alb_ids, item['id'])
    for alb_id in alb_ids:
        album_url = f'https://api.spotify.com/v1/albums/{alb_id}/tracks'
        response_album = requests.get(album_url, headers = headers)  
        if response_album.status_code == 200:
            response_album = response_album.json()
            songs_per_album = np.append(songs_per_album, len(response_album['items']))
            for song in response_album['items']:
                track_ids = np.append(track_ids, song['id'])
else:
    print('Error ' + str(response_artists.status_code))

#print(len(alb_ids), len(track_ids), len(songs_per_album))
#Verifying Ids

if len(track_ids) == sum(songs_per_album) and len(alb_ids) == len(songs_per_album):
    print('Success')
else:
    print('Failure')

#Creating CSV file for extraction into Neo4j



batch1 = track_ids[:50]
batch2 = track_ids[50:100]
batch3 = track_ids[100:150]
batch4 = track_ids[150:200]
batch5 = track_ids[200:250]
batch6 = track_ids[250:300]
batch7 = track_ids[300:339]

batch1 = ",".join(batch1)
batch2 = ",".join(batch2)
batch3 = ",".join(batch3)
batch4 = ",".join(batch4)
batch5 = ",".join(batch5)
batch6 = ",".join(batch6)
batch7 = ",".join(batch7)

track_batches = np.array([batch1, batch2, batch3, batch4, batch5, batch6, batch7])

columns = ['Energy', 'Acousticness', 'Valence']
feature_data = pd.DataFrame(columns=columns)

columns = ['Name', 'Date']
info_data = pd.DataFrame(columns=columns)
for batch in track_batches:

    #API Call for getting Energy, Acoust, Valence
    audio_features_url = f'https://api.spotify.com/v1/audio-features?ids={batch}'
    response_features = requests.get(audio_features_url, headers = headers)
    #API Call for getting Name and Date
    track_info_url = f'https://api.spotify.com/v1/tracks?ids={batch}'
    response_trackinfo = requests.get(track_info_url, headers = headers)

    if response_features.status_code == 200 and response_trackinfo.status_code == 200:
        response_features = response_features.json()
        response_trackinfo = response_trackinfo.json()

        for response in response_features['audio_features']:
            new_row = pd.Series({'Energy': response['energy'],
                                'Acousticness': response['acousticness'],
                                'Valence': response['valence']})
            feature_data.loc[len(feature_data)] = new_row
        for response in response_trackinfo['tracks']:
            new_row = pd.Series({'Name': response['name'],
                                 'Date': response['album']['release_date']})
            info_data.loc[len(info_data)] = new_row
    else:
        print(response_features.status_code, response_trackinfo.status_code) #response_trackinfo.text)#response_features.text)

info_feature_data = pd.concat([info_data, feature_data], axis = 1)
print(info_feature_data.head(), len(info_feature_data))
#Info and Feature Data Extracted!!!!!!!!!

columns = ['Tempo', 'Key', 'Mode']
analysis_data = pd.DataFrame(columns=columns)

for id in track_ids:
    #API Call for getting Temp, Key, and Mode(major/minor)
    audio_analysis_url = f'https://api.spotify.com/v1/audio-analysis/{id}'
    response_analysis = requests.get(audio_analysis_url, headers=headers)

    if response_analysis.status_code == 200:
        response_analysis = response_analysis.json()
        new_row = pd.Series({'Tempo': response_analysis['track']['tempo'],
                             'Key': response_analysis['track']['key'],
                             'Mode': response_analysis['track']['mode']})
        analysis_data.loc[len(analysis_data)] = new_row
    else:
        print(response_analysis.status_code, response_analysis.text)

final_data = pd.concat([info_feature_data, analysis_data], axis = 1)
print(final_data.head(), len(final_data))
final_data.to_csv('Spotfy_EdSheeran_Test.csv', index=False)





