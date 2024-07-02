import numpy as np
import pandas as pd

data = pd.read_csv('Spotify_Beatles.csv')
#print(data)

def find_all_indices(substring, string):
    indices = []
    start = 0
    while True:
        start = string.find(substring, start)
        if start == -1:
            break
        indices.append(start)
        start += len(substring)  # Use start += 1 to find overlapping matches
    return indices


drop_indices = []

for i in range(len(data)):
    #We want to exclude all of the non-final versions of songs, which are labeled as Takes
    if 'Take' in data.loc[i, 'Name']:
        drop_indices.append(i)
    else:
        #The Songs are formatted such that they will have the name of the song to the left of the dash, and additional info to the right
        dash_indices = find_all_indices('-', data.loc[i, 'Name'])
        if len(dash_indices) == 1:
            data.loc[i, 'Name'] = data.loc[i, 'Name'][:dash_indices[0]-1]
        #1st Exception to the above rule, since this song name contains additional dashes
        elif 'Ob-La-Di, Ob-La-Da' in data.loc[i, 'Name']:
            data.loc[i, 'Name'] = data.loc[i, 'Name'][:dash_indices[4]-1]
        #This is an inconsistency in the song name, where some versions have the dash between sixty four, and others don't
        elif 'Sixty-Four' in data.loc[i, 'Name']:
            data.loc[i, 'Name'] = "When I'm Sixty Four"
        #The only instances when information appears to the left of the dash, is with the couple of rows which have (reprise)
        if '(Reprise)' in data.loc[i, 'Name']:
            index = data.loc[i, 'Name'].find('(Reprise)')
            data.loc[i, 'Name'] = data.loc[i, 'Name'][:index-1]
        #Songs with the / in the name appear to be a mix of Beatles Songs, or workshopping sessions.
        if '/' in data.loc[i, 'Name']:
            drop_indices.append(i)


data.drop(drop_indices, inplace=True)
data.reset_index(drop=True, inplace=True)
print(len(data))
#Changing Minor Punctuation Inconsistencies
data.loc[139, 'Name'] = "Baby, You're A Rich Man"
data.loc[15, 'Name'] = "Back In The U.S.S.R."
data.loc[137, 'Name'] = "Sgt. Pepper's Lonely Hearts Club Band"

#Dropping Songs versions made by George Martin, not a core member of the Beatles
data.drop([236, 237], inplace=True)
data.reset_index(drop=True, inplace=True)

#Converting dates from strings to datetime objects for easy comparison
data['Date'] = pd.to_datetime(data['Date'])

#Sorting data by song name and release date
data = data.sort_values(by = ['Name', 'Date'])

#Dropping the duplicates and keeping only the version with the oldest release date
data = data.drop_duplicates(subset='Name', keep='first')
data.reset_index(drop=True, inplace=True)

#print(len(data), data)

#Standardizing data subset
subset = pd.DataFrame({'Name': data['Name'], 'Date': data['Date'], 'Energy': data['Energy'], 'Tempo': data['Tempo'], 'Key': data['Key']})

def standardize(df):
    new_df = pd.DataFrame()
    for cols in df.columns:
        new_col = []
        for i in range(len(df)):
            temp = (df.loc[i, cols] - np.mean(df[cols])) / np.std(df[cols])
            new_col.append(temp)
        new_df[cols] = new_col
    
    return new_df

temp = standardize(subset[['Energy', 'Tempo', 'Key']])
subset.drop(columns=['Energy', 'Tempo', 'Key'], inplace=True)
for cols in temp.columns:
    subset[cols] = temp[cols]

#print(subset)
subset.to_csv('Spotify_Standardized.csv', index=False)    




