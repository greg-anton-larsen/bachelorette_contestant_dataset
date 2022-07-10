import urllib.request
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
import numpy as np
import re
import sqlite3

# Extract an HTML table of contestants from a given season's Wikipedia page
def fetch_table(count):
    stem = 'https://en.wikipedia.org/wiki/The_Bachelorette_(American_season_{})'.format(count)
    raw_page = urllib.request.urlopen(stem)
    html = raw_page.read()
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('table', class_='wikitable')[0]
    return table

# Parse the HTML table to get raw information about each contestant
# Yielding, for each contestant, a dict with labeled pieces of information
def table_to_contestant_list(table):
    contestant_list = []
    rows = table.find_all('tr')
    headers = rows[0].find_all('th')
    columns = [head.contents[0].strip() for head in headers]

    for row in rows[1:]:
            contestant = {}
            vals = row.find_all('td')
            for label, cell in zip(columns,vals):
                if cell.find('b'):
                    cell = cell.find('b')
                if cell.find('a'):
                    if len(cell.find_all('sup')) == len(cell.find_all('a')):
                        val = cell.contents[0]
                    else:
                        val = cell.find('a').contents[0].strip()                           
                else:
                    val = cell.contents[0].strip()
                contestant[label] = str(val)
            contestant_list.append(contestant)
    return contestant_list

# Between the seasons, each table has slightly different fields
# with some different column names, occasional columns I'm not including, and formatting peculiarities
def standardize_fields(season, contestant_list):
    # rename columns that had different names between each season's table
    for contestant in contestant_list:
        if season == 1:
            contestant['Occupation'] = contestant['Job']
            contestant.pop('Job')
        if season < 4:
            if 'Eliminated' in contestant:
                contestant['Outcome'] = contestant['Eliminated']
                contestant.pop('Eliminated')
    
    # deal with the fact that the tables after Season 3 have uneven numbers of fields per tr
    # this is to indicate that multiple contestants shared the same place and/or outcome
    # i.e. saying that contestants tied for 5-8th place by being eliminated at the same time
    if season > 3:
        for x in range(len(contestant_list)):
            if 'Place' not in contestant_list[x] or 'sup' in contestant_list[x]['Place']:              
                if 'Outcome' in contestant_list[x]:
                    contestant_list[x]['Place'] = contestant_list[x]['Outcome']
                    contestant_list[x]['Outcome'] = ''
                else:
                    contestant_list[x]['Place'] = contestant_list[x-1]['Place']
                    contestant_list[x]['Outcome'] = ''
        
    return contestant_list


# Neatens the 'Outcome' values, filling in missing values due to formatting
# And makes a few tweaks to fix unusual seasons' tables
def tidy_outcome(table):
    tidy_outcome = list(table['Outcome'])
    for i in range(1,len(tidy_outcome)):
        tidy_outcome[i] = tidy_outcome[i].replace("Episode","Week")
        if not tidy_outcome[i]:
            tidy_outcome[i] = tidy_outcome[i-1]
    if table['Season'][0] == 16:
        tidy_outcome[0:3] = ["Clare's Winner", "Tayshia's Winner", "Tayshia's Runner-Up"]
    return tidy_outcome


# Neatens the 'Place' values, removing extraneous info
# Also fills in missing values due to the table's formatting (when contestants tied)
def tidy_place(table):
    if table.Season[0] < 4:
        tidy_place = np.array(table.index) + 1
        for i in range(1,len(tidy_place)):
            if table['Outcome'][i] == table['Outcome'][i-1]:
                prev = i-1
                tidy_place[i] = tidy_place[prev]
        return tidy_place
    else:
        tidy_place = table['Place'].apply(lambda x: re.split('[-–\s]',x)[0])
        return tidy_place
    
    
# A wrapper function for gathering information for a given season, calling all the previous functions
# And putting the output in the database
def create_contestant_records(season_number):
    print(season_number)
    table = fetch_table(season_number)
    contestant_list = table_to_contestant_list(table)
    neatened_columns = standardize_values(season_number, contestant_list)
    dataframe = pd.DataFrame(neatened_columns, columns = ['Season','Name','Age','Hometown','Occupation','Outcome','Place'])
    dataframe['Season'] = season_number
    dataframe['Outcome'] = tidy_outcome(dataframe)
    dataframe['Place'] = tidy_place(dataframe)
    conn = sqlite3.connect("bachelorette.db")
    dataframe.to_sql('contestants', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    time.sleep(1)
    return dataframe


# Initializing and refilling the SQLITE3 database
def reset_database():
    conn = sqlite3.connect("bachelorette.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS contestants")
    cur.execute('''CREATE TABLE contestants
               (season integer, name text, age integer, occupation text, hometown text, outcome text, place int)''')
    conn.commit()
    conn.close()
    [create_contestant_records(i) for i in range(1,20)]