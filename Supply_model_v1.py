import os
import requests
import json
import pandas as pd
from datetime import date, timedelta, datetime
import time
import schedule
import ssl
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
import io
import zipfile
import numpy as np
import glob
from openpyxl import load_workbook
import re
import tkinter as tk
import warnings



def  Jamal_Mallnow(yesterday, today, data_supply):
    url = 'https://tron.gascade.biz/ibis/servlet/IBISHTTPUploadServlet/PFW_Webapp_Funktionen?_dc=1640006811911'
    payload = {
    "language": "en",
    "mandant": "GASCADE",
    "data": "<data><from>"+str(yesterday)+"</from><to>"+str(today)+"</to><granularity>Gastage</granularity><collapsed>false</collapsed><timeSeries><item><pointID>21Z000000000056S</pointID><flowType>Entry</flowType><timeSeriesCode>Nomination</timeSeriesCode></item><item><pointID>21Z000000000056S</pointID><flowType>Exit</flowType><timeSeriesCode>Nomination</timeSeriesCode></item><item><pointID>21Z000000000056S</pointID><flowType>Entry</flowType><timeSeriesCode>Renomination</timeSeriesCode></item><item><pointID>21Z000000000056S</pointID><flowType>Exit</flowType><timeSeriesCode>Renomination</timeSeriesCode></item><item><pointID>21Z000000000056S</pointID><flowType>Entry</flowType><timeSeriesCode>PhysicalFlow</timeSeriesCode></item><item><pointID>21Z000000000056S</pointID><flowType>Exit</flowType><timeSeriesCode>PhysicalFlow</timeSeriesCode></item></timeSeries></data>",
    "operation": "timeSeries",
    "page": "1",
    "start": "0",
    "limit": "25"
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}         

    response = requests.post(url, data = payload, headers=headers, allow_redirects=True)
    data = response.json()
    
    malnow = 'Mallnow to DE (entry-exit)'
    rus = 'RUS'

    temp = data_supply[rus][malnow]

    for dat in data['data']:
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Nomination'] = (int(dat['values'][0]) - int(dat['values'][1]))*(10**-6)
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Renomination'] = (int(dat['values'][2]) - int(dat['values'][3]))*(10**-6)
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Flow'] = (int(dat['values'][4]) - int(dat['values'][5]))*(10**-6) if dat['values'][4] != '' and dat['values'][5] != ''  else np.NaN


    data_supply.loc[data_supply[rus].index.get_level_values(0), data_supply.columns.get_level_values(1) == malnow] = temp.values
    return data_supply

def  NEL(yesterday, today, data_supply):
    url = 'https://tron.nel-gastransport.biz/ibis/servlet/IBISHTTPUploadServlet/PFW_Webapp_Funktionen?_dc=1642071143076'
    payload = {
    "language": "en",
    "mandant": "NEL",
    "data": "<data><from>"+str(yesterday)+"</from><to>"+str(today)+"</to><granularity>Gastage</granularity><collapsed>false</collapsed><timeSeries><item><pointID>21Z000000000255M</pointID><flowType>Entry</flowType><timeSeriesCode>Nomination</timeSeriesCode></item><item><pointID>21Z000000000255M</pointID><flowType>Entry</flowType><timeSeriesCode>Renomination</timeSeriesCode></item><item><pointID>21Z000000000255M</pointID><flowType>Entry</flowType><timeSeriesCode>PhysicalFlow</timeSeriesCode></item></timeSeries></data>",
    "operation": "timeSeries",
    "page": "1",
    "start": "0",
    "limit": "25"
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}         

    response = requests.post(url, data = payload, headers=headers, allow_redirects=True)
    data = response.json()
    
    data_from = []
    data_to = []
    data_nomEntry = []
    data_renomEntry = []
    data_physEntry = []
    date = []

    temp = data_supply['RUS']['Nord stream - NEL']

    for dat in data['data']:

        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Flow'] = int(dat['values'][2])*(10**-6) if  dat['values'][2] != ''  else np.NaN
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Nomination'] = int(dat['values'][0])*(10**-6) if  dat['values'][0] != ''  else np.NaN
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Renomination'] = int(dat['values'][1])*(10**-6) if  dat['values'][1] != ''  else np.NaN

        data_from.append(dat['from'])
        data_to.append(dat['to'])
        date.append(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d'))
        data_nomEntry.append(dat['values'][0])
        data_renomEntry.append(dat['values'][1])
        data_physEntry.append(dat['values'][2])
        
    
    df = {'From': data_from,
          'To': data_to,
          'Date': date,
          'Greifswald-NEL Nomination Entry (kWh/d)': data_nomEntry,
          'Greifswald-NEL Renomination Entry (kWh/d)': data_renomEntry,
          'Greifswald-NEL PhysicalFlow Entry (kWh/d)': data_physEntry
        }
    df = pd.DataFrame(df)
    
    data_supply.loc[data_supply['RUS'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Nord stream - NEL'] = temp.values

    #print(data_supply['RUS']['Nord stream - NEL'].head(40))

    return data_supply


def  OPAL(yesterday, today, data_supply):
    url = 'https://tron.opal-gastransport.biz/ibis/servlet/IBISHTTPUploadServlet/PFW_Webapp_Funktionen?_dc=1642072145057'
    payload = {
    "language": "en",
    "mandant": "OPAL",
    "data": "<data><from>"+str(yesterday)+"</from><to>"+str(today)+"</to><granularity>Gastage</granularity><collapsed>false</collapsed><timeSeries><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Nomination-R</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Nomination-PR</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Nomination-NR</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Renomination-R</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Renomination-PR</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>Renomination-NR</timeSeriesCode></item><item><pointID>21Z000000000241X</pointID><flowType>Entry</flowType><timeSeriesCode>PhysicalFlow</timeSeriesCode></item></timeSeries></data>",
    "operation": "timeSeries",
    "page": "1",
    "start": "0",
    "limit": "25"
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}         

    response = requests.post(url, data = payload, headers=headers, allow_redirects=True)
    data = response.json()
    
    data_from = []
    data_to = []
    data_nomEntryReg = []
    data_nomEntryPart = []
    data_nomEntryNot = []
    data_renomEntryReg = []
    data_renomEntryPart = []
    data_renomEntryNot = []
    data_physEntry = []

    temp = data_supply['RUS']['Nord stream - OPAL']

    for dat in data['data']:

        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Flow'] = int(dat['values'][6])*(10**-6) if  dat['values'][6] != ''  else np.NaN
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Nomination'] = int(dat['values'][2])*(10**-6) if  dat['values'][2] != ''  else np.NaN
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat['from'], '%m/%d/%Y %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Renomination'] = int(dat['values'][5])*(10**-6) if  dat['values'][5] != ''  else np.NaN


        data_from.append(dat['from'])
        data_to.append(dat['to'])
        data_nomEntryReg.append(dat['values'][0])
        data_nomEntryPart.append(dat['values'][1])
        data_nomEntryNot.append(dat['values'][2])
        data_renomEntryReg.append(dat['values'][3])
        data_renomEntryPart.append(dat['values'][4])
        data_renomEntryNot.append(dat['values'][5])
        data_physEntry.append(dat['values'][6])
       
    
    df = {'From': data_from,
          'To': data_to,
          'Greifswald-OPAL Nomination (regulated) Entry (kWh/d)': data_nomEntryReg,
          'Greifswald-OPAL Nomination (partly regulated) Entry (kWh/d)': data_nomEntryPart,
          'Greifswald-OPAL Nomination (not regulated) Entry (kWh/d)': data_nomEntryNot,
          'Greifswald-OPAL Renomination (regulated) Entry (kWh/d)': data_renomEntryReg,
          'Greifswald-OPAL Renomination (partly regulated) Entry (kWh/d)': data_renomEntryPart,
          'Greifswald-OPAL Renomination (not regulated) Entry (kWh/d)': data_renomEntryNot,
          'Greifswald-OPAL PhysicalFlow Entry (kWh/d)': data_physEntry
        }
    df = pd.DataFrame(df)

    data_supply.loc[data_supply['RUS'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Nord stream - OPAL'] = temp.values
    #print(data_supply['RUS']['Nord stream - OPAL'].head(40))
    return data_supply

def VIP_Bereg_UA_HU(yesterday, today, data_supply):

    day_from = yesterday.strftime("%Y-%m-%dT%H:%M:%S")
    day_to = today.strftime("%Y-%m-%dT%H:%M:%S")

    url = 'https://ipnew.rbp.eu/Fgsz.Tso.Data.Web/api/TsoData/GetFactDailySetList?_dc=1639988317972'
    payload = {
    "fields": None,
    "filter": [
        {
            "comparison": "bw",
            "property": "gasDayRange",
            "values": [
                str(day_from),
                str(day_to)
            ]
        },
        {
            "comparison": "==",
            "property": "dimNetworkPointId",
            "value": 318
        },
        {
            "comparison": "==",
            "property": "unit",
            "value": "kwh"
        },
        {
            "comparison": "in",
            "property": "dimValueTypeId",
            "values": [
                20,
                22,
                26
            ]
        }
    ],
    "limit": 2000,
    "sort": [
        {
            "direction": "DESC",
            "isGrouper": False,
            "property": "gasDay"
        }
    ],
    "start": 0
    }
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps(payload)
    response = requests.post(url, data = payload, headers=headers, allow_redirects=True)
    data = response.json()


    temp = data_supply['RUS']['FGSZ - VIP Bereg UA-HUN']

    for dat in data['data']:
        if dat['dimValueTypeName'] == 'Fizikai gázáram/Physical flow(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Flow'] = dat['value']*(10**-6) if 'value' in dat else ''
        elif dat['dimValueTypeName'] == 'Nominálás/Nomination(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Nomination'] = dat['value']*(10**-6) if 'value' in dat else ''
        elif dat['dimValueTypeName'] == 'Újranominálás/Renomination(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Renomination'] = dat['value']*(10**-6) if 'value' in dat else ''

    temp.replace('NaN', np.NaN)
    temp.replace('', np.NaN)

    data_supply.loc[data_supply['RUS'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'FGSZ - VIP Bereg UA-HUN'] = temp.values

    #print(data_supply['RUS']['FGSZ - VIP Bereg'].head(40))
    return data_supply

def VIP_Bereg_HU_UA(yesterday, today, data_supply):

    day_from = yesterday.strftime("%Y-%m-%dT%H:%M:%S")
    day_to = today.strftime("%Y-%m-%dT%H:%M:%S")

    url = 'https://ipnew.rbp.eu/Fgsz.Tso.Data.Web/api/TsoData/GetFactDailySetList?_dc=1645122256534'
    payload = {
    "fields": None,
    "filter": [
        {
            "comparison": "bw",
            "property": "gasDayRange",
            "values": [
                str(day_from),
                str(day_to)
            ]
        },
        {
            "comparison": "==",
            "property": "dimNetworkPointId",
            "value": 319
        },
        {
            "comparison": "==",
            "property": "unit",
            "value": "kwh"
        },
        {
            "comparison": "in",
            "property": "dimValueTypeId",
            "values": [
                20,
                22,
                26
            ]
        }
    ],
    "limit": 2000,
    "sort": [
        {
            "direction": "DESC",
            "isGrouper": False,
            "property": "gasDay"
        }
    ],
    "start": 0
    }
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps(payload)
    response = requests.post(url, data = payload, headers=headers, allow_redirects=True)
    data = response.json()


    temp = data_supply['RUS']['FGSZ - VIP Bereg HUN-UA']

    for dat in data['data']:
        if dat['dimValueTypeName'] == 'Fizikai gázáram/Physical flow(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Flow'] = dat['value']*(10**-6) if 'value' in dat else ''
        elif dat['dimValueTypeName'] == 'Nominálás/Nomination(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Nomination'] = dat['value']*(10**-6) if 'value' in dat else ''
        elif dat['dimValueTypeName'] == 'Újranominálás/Renomination(kWh 25°/0°)':
            temp.loc[temp.index.get_level_values(0) == dat['gasPeriod'], temp.columns.get_level_values(0) == 'Renomination'] = dat['value']*(10**-6) if 'value' in dat else ''

    temp.replace('NaN', np.NaN)
    temp.replace('', np.NaN)
    data_supply.loc[data_supply['RUS'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'FGSZ - VIP Bereg HUN-UA'] = temp.values

    #print(data_supply['RUS']['FGSZ - VIP Bereg'].head(40))
    return data_supply


def jamal_kondratki(yesterday, today, data_supply):
    
    yesterday = yesterday.strftime('%Y%m%d')
    today = today.strftime('%Y%m%d')

    url_flow = 'https://swi.gaz-system.pl/swi/public/api/actualQuantity?cacheBuster=1644312690850&allItems=false&columns=&count=25&filtering%5Bday%5D=ge'+yesterday+',le'+today+'&filtering%5Bid%5D=870001&lang=en&operator=SGTT&page=0&sorting=dday,aid'
    response_flow = requests.get(url_flow)
    data_flow = response_flow.json()

    url_nom = 'https://swi.gaz-system.pl/swi/public/api/reNomDaily?cacheBuster=1644307851749&allItems=false&columns=&count=25&filtering%5BgasWeek%5D=ge'+yesterday+',le'+today+'&filtering%5Bid%5D=870001&lang=en&operator=SGTT&page=0&pointPreset=pointIds&renomination=false&sorting=dgasWeek,aid'
    response_nom = requests.get(url_nom)
    data_nom = response_nom.json()

    url_renom = 'https://swi.gaz-system.pl/swi/public/api/reNomDaily?cacheBuster=1644308133862&allItems=false&columns=&count=25&filtering%5BgasWeek%5D=ge'+yesterday+',le'+today+'&filtering%5Bid%5D=870001&lang=en&operator=SGTT&page=0&pointPreset=pointIds&renomination=true&sorting=dgasWeek,aid'
    response_renom = requests.get(url_renom)
    data_renom = response_renom.json()

    temp = data_supply['RUS']['Kondratki']

    for dat in data_flow['items']:
        temp.loc[temp.index.get_level_values(0) == dat['day'], temp.columns.get_level_values(0) == 'Flow'] = int(dat['allocationE'].replace(" ", ""))*(10**-6)

    for dat_nom in data_nom['items']:
        temp.loc[temp.index.get_level_values(0) == dat_nom['gasWeek'], temp.columns.get_level_values(0) == 'Nomination'] = int(dat_nom['nominationE'].replace(" ", ""))*(10**-6)

    for dat_renom in data_renom['items']:
        temp.loc[temp.index.get_level_values(0) == dat_renom['gasWeek'], temp.columns.get_level_values(0) == 'Renomination'] = int(dat_renom['renominationE'].replace(" ", ""))*(10**-6)

    data_supply.loc[data_supply['RUS'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Kondratki'] = temp.values


    return data_supply


def gassco(today, data_supply):

    url = 'https://umm.gassco.no/'

    headers = { 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'sk,cs;q=0.8,en-US;q=0.5,en;q=0.3',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Cookie': '_ga=GA1.2.1650529501.1640164380; Igng4tfXGSb8T3XfxU=1641896275553:1; JSESSIONID=26F684D6AEB3266A491ADA21804E5C81; TS015e0c8d=01218a59f1eb233250e280f4b83e1cac24352a2f94848dd95c4b63cd684dd182fd468b322238e1b1e987c06ddcdeccc1e675e38e9601259eed60259533e16532982c16edda8a7423a8f53cc8924d4f1801758bc2f0; _gid=GA1.2.310581584.1641895543',
                'Host': 'umm.gassco.no',
                'Referer': 'https://umm.gassco.no/disclaimer',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'}

    response = requests.get(url, headers=headers)

    dfs = pd.read_html(response.text)
    df = dfs[1]

    nor = data_supply['NOR']

    for i in range(0,9):
        items = df[i][2].rsplit(' ', 3)
        if items[0].strip() == 'St.Fergus':
            items[0] = 'St. Fergus'
        elif items[0].strip() == 'Fields Delivering into SEGAL':
            items[0] = 'Segal'

        for col in nor.columns.get_level_values(0):
            if items[0][0].strip() in col:
                temp = nor[col]
                temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'real-time flow'] = int(round(float(items[1].strip().replace(",","."))))*11

                nor.loc[nor.index.get_level_values(0), nor.columns.get_level_values(0) == col] = temp.values

    data_supply.loc[data_supply['NOR'].index.get_level_values(0), data_supply.columns.get_level_values(0) == 'NOR'] = nor.values

    return data_supply

def snam(today, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context

    today_index = today.strftime("%B %d %Y")

    url = 'https://www.snam.it/en/transportation/operational-data-business/0-Phisical_Flows_on_the_national_network/'
    excel_url = 'https://www.snam.it/en/transportation/utilities/flussi-fisici/storico-download-excel-flussi-fisici.xlsx'
    headers = { 'User-Agent': 'Mozilla/5.0'}

    df = pd.read_excel(excel_url, skiprows=1, sheet_name='kWh', storage_options=headers)
    df = df.drop(range(22,34))
    df = df.rename(columns={'Unnamed: 1':'Flow', 'Gas day: '+str(today_index): 'Points'})
    df = df.drop(columns='Unnamed: 2')
    df = df.drop(0)

    df['Points'] = df['Points'].fillna(method='ffill')

    values_lib = 0
    lib_poc = 0
    values_mazara = 0
    mazara_poc = 0
    values_melendugno = 0
    melendungo_poc = 0
    paso_exit = 0
    paso_entry = 0
    paso_poc = 0
    travisio_exit = 0
    travisio_entry = 0
    travisio_poc = 0

    temp_lib = data_supply['Libya']['Gela (LB-IT)']
    temp_mazara = data_supply['Algeria']['Mazara del Vallo (ALG-IT)']
    temp_melendugno = data_supply['Azeri']['TAP Melendugno(ITA)']
    temp_pas = data_supply['cross-borders']['Passo Gries (entry-exit)']
    temp_travisio = data_supply['cross-borders']['Tarvisio (entry-exit)']
    
    for col in df.columns:
        if col != 'Points' and col != 'Flow':
            if len(df[(df['Points'] == 'Gela') & (df['Flow'] == 'entry') & (df[col].notna())][col].values) > 0:
                values_lib += df[(df['Points'] == 'Gela') & (df['Flow'] == 'entry') & (df[col].notna())][col].values[0]
                lib_poc += 1
            if len(df[(df['Points'] == 'Mazara del Vallo') & (df['Flow'] == 'entry') & (df[col].notna())][col].values) > 0:
                values_mazara += df[(df['Points'] == 'Mazara del Vallo') & (df['Flow'] == 'entry') & (df[col].notna())][col].values[0]
                mazara_poc += 1
            if len(df[(df['Points'] == 'Melendugno') & (df['Flow'] == 'entry') & (df[col].notna())][col].values) > 0:
                values_melendugno += df[(df['Points'] == 'Melendugno') & (df['Flow'] == 'entry') & (df[col].notna())][col].values[0]
                melendungo_poc += 1
            if len(df[(df['Points'] == 'Passo Gries') & (df[col].notna())][col].values) > 0:
                paso_exit += df[(df['Points'] == 'Passo Gries') & (df['Flow'] == 'exit') & (df[col].notna())][col].values[0]
                paso_entry += df[(df['Points'] == 'Passo Gries') & (df['Flow'] == 'entry') & (df[col].notna())][col].values[0]
                paso_poc += 1
            if len(df[(df['Points'] == 'Tarvisio') & (df[col].notna())][col].values) > 1:
                travisio_exit += df[(df['Points'] == 'Tarvisio') & (df['Flow'] == 'exit') & (df[col].notna())][col].values[0] if df[(df['Points'] == 'Tarvisio') & (df['Flow'] == 'exit') & (df[col].notna())][col].values[0] != '-' else  0    
                travisio_entry += df[(df['Points'] == 'Tarvisio') & (df['Flow'] == 'entry') & (df[col].notna())][col].values[0]
                travisio_poc += 1
            else:
                break 
    
    temp_lib.loc[temp_lib.index.get_level_values(0) == str(today), temp_lib.columns.get_level_values(0) == 'real-time flow'] = (values_lib / lib_poc)*24*(10**-6) if lib_poc > 0 else 'NaN'
    temp_mazara.loc[temp_mazara.index.get_level_values(0) == str(today), temp_mazara.columns.get_level_values(0) == 'real-time flow'] = (values_mazara / mazara_poc)*24*(10**-6) if mazara_poc > 0 else 'NaN'
    temp_melendugno.loc[temp_melendugno.index.get_level_values(0) == str(today), temp_melendugno.columns.get_level_values(0) == 'real-time flow'] = (values_melendugno / melendungo_poc)*24*(10**-6) if melendungo_poc > 0 else 'NaN'
    temp_pas.loc[temp_pas.index.get_level_values(0) == str(today), temp_pas.columns.get_level_values(0) == 'Flow'] = ((paso_entry/paso_poc) - (paso_exit/paso_poc))*24*(10**-6) if paso_poc > 0 else 'NaN'
    temp_travisio.loc[temp_travisio.index.get_level_values(0) == str(today), temp_travisio.columns.get_level_values(0) == 'Flow'] = ((travisio_entry/travisio_poc) - (travisio_exit/travisio_poc))*24*(10**-6) if travisio_poc > 0 else 'NaN'
    
    data_supply.loc[data_supply['Libya'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Gela (LB-IT)'] = temp_lib.values
    data_supply.loc[data_supply['Algeria'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Mazara del Vallo (ALG-IT)'] = temp_mazara.values
    data_supply.loc[data_supply['Azeri'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'TAP Melendugno(ITA)'] = temp_melendugno.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Passo Gries (entry-exit)'] = temp_pas.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Tarvisio (entry-exit)'] = temp_travisio.values


    return data_supply



def enagas(yesterday, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context
    
    day =  yesterday.strftime("%d/%m/%Y")

    excel_url = 'https://www.enagas.es/web-corporativa-ext-templating/webcorp/informePPD/getPPDExcel?language=en&fecha='+str(day)

    df = pd.read_excel(excel_url, skiprows=range(1, 23), usecols='C:K')
    df_cont = df.drop(range(10, 27))
    df_cont = df_cont.rename(columns={'Unnamed: 2':'LNG Terminals', 'Unnamed: 3':'Value Type', 'Unnamed: 4':'Total', 'Unnamed: 5':'Barcelona', 'Unnamed: 6':'Cartagena', 'Unnamed: 7':'Huelva', 'Unnamed: 8':'BBG', 'Unnamed: 9':'Sagundo', 'Unnamed: 10':'Reganosa'})
    df_cont['LNG Terminals'] = df_cont['LNG Terminals'].fillna(method='ffill')

    df_inter = df.drop(range(0, 13))
    df_inter = df_inter.drop(range(14, 27))
    df_inter = df_inter.drop(columns=['Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 9', 'Unnamed: 10'])
    df_inter = df_inter.rename(columns={'Unnamed: 2':'Terminal', 'Unnamed: 7':'TARIFA', 'Unnamed: 8':'ALMERÍA'})
    df_inter['Terminal'] = df_inter['Terminal'].fillna('Border Interconnection Points')

    temp_almeria = data_supply['Algeria']['Almeria (ALG-ESP)']
    temp_tarifa = data_supply['Algeria']['Tarifa (ALG-ESP)']
    temp_barcelona = data_supply['LNG']['Barcelona (ESP)']
    temp_cartagena = data_supply['LNG']['Cartagena (ESP)']
    temp_huelva = data_supply['LNG']['Huelva (ESP)']
    temp_bbg = data_supply['LNG']['BBG (ESP)']
    temp_sagundo = data_supply['LNG']['Sagundo (ESP)']
    temp_reganosa = data_supply['LNG']['Reganosa (ESP)']
    
    temp_almeria.loc[temp_almeria.index.get_level_values(0) == str(yesterday), temp_almeria.columns.get_level_values(0) == 'Flow'] = df_inter['ALMERÍA'].values[0]
    temp_tarifa.loc[temp_tarifa.index.get_level_values(0) == str(yesterday), temp_tarifa.columns.get_level_values(0) == 'Flow'] = df_inter['TARIFA'].values[0]
    
    temp_barcelona.loc[temp_barcelona.index.get_level_values(0) == str(yesterday), temp_barcelona.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['Barcelona'].values[0]
    temp_barcelona.loc[temp_barcelona.index.get_level_values(0) == str(yesterday), temp_barcelona.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['Barcelona'].values[0] 
    
    temp_cartagena.loc[temp_cartagena.index.get_level_values(0) == str(yesterday), temp_cartagena.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['Cartagena'].values[0] 
    temp_cartagena.loc[temp_cartagena.index.get_level_values(0) == str(yesterday), temp_cartagena.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['Cartagena'].values[0] 
    
    temp_huelva.loc[temp_huelva.index.get_level_values(0) == str(yesterday), temp_huelva.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['Huelva'].values[0] 
    temp_huelva.loc[temp_huelva.index.get_level_values(0) == str(yesterday), temp_huelva.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['Huelva'].values[0] 
    
    temp_bbg.loc[temp_bbg.index.get_level_values(0) == str(yesterday), temp_bbg.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['BBG'].values[0] 
    temp_bbg.loc[temp_bbg.index.get_level_values(0) == str(yesterday), temp_bbg.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['BBG'].values[0] 
    
    temp_sagundo.loc[temp_sagundo.index.get_level_values(0) == str(yesterday), temp_sagundo.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['Sagundo'].values[0] 
    temp_sagundo.loc[temp_sagundo.index.get_level_values(0) == str(yesterday), temp_sagundo.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['Sagundo'].values[0] 
    
    temp_reganosa.loc[temp_reganosa.index.get_level_values(0) == str(yesterday), temp_reganosa.columns.get_level_values(0) == 'Initial Inventory tanks'] = df_cont[(df_cont['LNG Terminals'] == 'Initial Inventory tanks') & (df_cont['Value Type'] == 'GWh/day')]['Reganosa'].values[0] 
    temp_reganosa.loc[temp_reganosa.index.get_level_values(0) == str(yesterday), temp_reganosa.columns.get_level_values(0) == 'Regasification'] = df_cont[(df_cont['LNG Terminals'] == 'Regasification') & (df_cont['Value Type'] == 'GWh/day')]['Reganosa'].values[0] 
    
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Barcelona (ESP)'] = temp_barcelona.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Cartagena (ESP)'] = temp_cartagena.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Huelva (ESP)'] = temp_huelva.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'BBG (ESP)'] = temp_bbg.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Sagundo (ESP)'] = temp_sagundo.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Reganosa (ESP)'] = temp_reganosa.values
    data_supply.loc[data_supply['Algeria'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Almeria (ALG-ESP)'] = temp_almeria.values
    data_supply.loc[data_supply['Algeria'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Tarifa (ALG-ESP)'] = temp_tarifa.values

    return data_supply




def grtgaz(yesyesterday, yesterday, today, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context
    excel_url = 'https://smart.grtgaz.com/api/v1/en/flux_physiques/export/PITTM.xls?startDate='+str(yesyesterday)+'&endDate='+str(today)+'&range=daily'
    headers = { 'User-Agent': 'Mozilla/5.0'}

    df = pd.read_excel(excel_url, skiprows=3, storage_options=headers)
    df = df.rename(columns={'Unnamed: 0':'Date'})


    lng = data_supply['LNG']
    temp = lng['Dunkerque (FRA)']

    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df[df['Date'] == str(yesterday)]['Measure'].values[0]*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'Flow'] = df[df['Date'] == str(yesyesterday)]['Measure'].values[0]*(10**-6)
    lng.loc[lng.index.get_level_values(0), lng.columns.get_level_values(0) == 'Dunkerque (FRA)'] = temp.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(0) == 'LNG'] = lng.values

    return data_supply

def gateterminal(yesterday, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context
    excel_url = 'https://www.gateterminal.com/wp-content/themes/minimal210-child/Huidig-gebruik-bestand/gate_stats.xls'
    headers = { 'User-Agent': 'Mozilla/5.0'}

    df = pd.read_excel(excel_url, storage_options=headers)   

    temp = data_supply['LNG']['Gate (NL)']
    if df[df['Date (dd/mm/yyyy)'] == str(yesterday)]['Amount of Gas in LNG facility (GWh)'].values: 
        temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Storage'] = df[df['Date (dd/mm/yyyy)'] == str(yesterday)]['Amount of Gas in LNG facility (GWh)'].values[0]
    if df[df['Date (dd/mm/yyyy)'] == str(yesterday)]['Outflow (GWh) '].values: 
        temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Outflow'] = df[df['Date (dd/mm/yyyy)'] == str(yesterday)]['Outflow (GWh) '].values[0]
    data_supply.loc[data_supply['LNG'].index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Gate (NL)'] = temp.values

    return data_supply

def fosmax(yesterday, today, data_supply):   
    excel_url = 'https://www.fosmax-lng.com/en/our-services/operational-data/usage-data/recherches.html?article1=10&article2=65'
    headers = { 'Content-Type':'application/x-www-form-urlencoded' }
    payload = { 
	"jform[terminal]": "1",
	"jform[jour1]": str(yesterday.day),
	"jform[mois1]": str(yesterday.month),
	"jform[annee1]": str(yesterday.year),
	"jform[jour2]": str(today.day),
	"jform[mois2]": str(today.month),
	"jform[annee2]": str(today.year),
	"jform[start]": "1",
	"jform[export]": "1",
	"submit": "View",
	"option": "com_transparence",
	"view": "recherches",
	"c4a327ae88b01caa04e2f2cd8c183995": "1"
    }
    
    response = requests.post(excel_url, headers=headers, data=payload)
    #df = pd.read_excel(response.text)
    with io.BytesIO(response.content) as fh:
        df = pd.io.excel.read_excel(fh, skiprows=range(0, 3), usecols='B:E')
    
    temp = data_supply['LNG']['Fos Cavaou (FRA)']
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Nomination'] = int(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0].replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Allocated Quant'] = int(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0].replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Nomination'] = int(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0].replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Allocated Quant'] = int(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0].replace(" ", ""))*(10**-6) if df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0] == "NaN" else np.NaN
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Fos Cavaou (FRA)'] = temp.values

    #print(data_supply['LNG']['Fos Cavaou (FRA)'].head(40))
    return data_supply

def fluxys(yesyesterday, yesterday, today, data_supply):
    day_from = yesyesterday.strftime("%d/%m/%Y")
    day_to = today.strftime("%d/%m/%Y")

    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(2)
    driver.get('https://gasdata.fluxys.com/SDP/Pages/Reports/Inventories.aspx?report=inventoriesLNG')
    link = driver.find_element_by_class_name('lang2')
    link.click()
    driver.get('https://gasdata.fluxys.com/SDP/Pages/Reports/Inventories.aspx?report=inventoriesLNG')
    #sleep(1)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, 'ctl00_ctl00_Content_ChildContentLeft_PeriodControl_PeriodToDatePicker'))).clear()
    driver.find_element_by_id('ctl00_ctl00_Content_ChildContentLeft_PeriodControl_PeriodFromDatePicker').clear()
    driver.find_element_by_id('ctl00_ctl00_Content_ChildContentLeft_PeriodControl_PeriodFromDatePicker').send_keys(str(day_from))
    driver.find_element_by_id('ctl00_ctl00_Content_ChildContentLeft_PeriodControl_PeriodToDatePicker').send_keys(str(day_to))
    driver.find_element_by_id('ctl00_ctl00_Content_LoadDataButton2').click()
    #sleep(1)
    table_html = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, 'VisibleReportContentctl00_ctl00_Content_ReportViewerControl_ctl09')))
    #table_html = driver.find_element_by_id('ctl00_ctl00_Content_ReportViewerControl_fixedTable')


    dfs = pd.read_html(table_html.get_attribute('innerHTML'))

    driver.quit()

    df = dfs[-4]
    df = df.drop(0)
    df = df.drop(columns=0)
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    lng = data_supply['LNG']
    temp = lng['Zeebrugge (BEL)']
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'GIS'] = int(df[df['Gas Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['GIS(kWh)'].values[0].replace("\xa0", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'DANSO'] = int(df[df['Gas Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['DANSO(kWh)'].values[0].replace("\xa0", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'PF'] = int(df[df['Gas Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['PF(kWh)'].values[0].replace("\xa0", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'GIS'] = int(df[df['Gas Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['GIS(kWh)'].values[0].replace("\xa0", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'DANSO'] = int(df[df['Gas Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['DANSO(kWh)'].values[0].replace("\xa0", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'PF'] = int(df[df['Gas Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['PF(kWh)'].values[0].replace("\xa0", ""))*(10**-6) if df[df['Gas Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['PF(kWh)'].values[0] == "NaN" else np.NaN
    lng.loc[lng.index.get_level_values(0), lng.columns.get_level_values(0) == 'Zeebrugge (BEL)'] = temp.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(0) == 'LNG'] = lng.values
    #print(data_supply['LNG']['Zeebrugge (BEL)'].head(40))
    return data_supply


def Fos_Tonkin(yesterday, today, data_supply):
    excel_url = 'https://www.elengy.com/en/contracts-and-operations/operational-management/use-data/recherches.html?article1=92&article2=106'
    headers = { 'Content-Type':'application/x-www-form-urlencoded' }
    payload = {
	"jform[terminal]": "2",
	"jform[jour1]": str(yesterday.day),
	"jform[mois1]": str(yesterday.month),
	"jform[annee1]": str(yesterday.year),
	"jform[jour2]": str(today.day),
	"jform[mois2]": str(today.month),
	"jform[annee2]": str(today.year),
	"jform[start]": "1",
	"jform[export]": "1",
	"submit": "View",
	"option": "com_transparence",
	"view": "recherches",
	"11c051d82963139790cb9094fb16e967": "1"
    }
    
    response = requests.post(excel_url, headers=headers, data=payload)
    #df = pd.read_excel(response.text)
    with io.BytesIO(response.content) as fh:
        df = pd.io.excel.read_excel(fh, skiprows=range(0, 3), usecols='B:E')

    temp = data_supply['LNG']['Fos Tonkin (FRA)']
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Nomination'] = float(str(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Allocated Quant'] = float(str(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Nomination'] = float(str(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Allocated Quant'] = float(str(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0]).replace(" ", ""))*(10**-6) if df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0]*(10**-6) == 'NaN' else np.NaN
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Fos Tonkin (FRA)'] = temp.values
    
    #print(data_supply['LNG']['Fos Tonkin (FRA)'].head(40))
    return data_supply

def Montoir(yesterday, today, data_supply):
    excel_url = 'https://www.elengy.com/en/contracts-and-operations/operational-management/use-data/recherches.html?article1=92&article2=106'
    headers = { 'Content-Type':'application/x-www-form-urlencoded' }
    payload = {
	"jform[terminal]": "1",
	"jform[jour1]": str(yesterday.day),
	"jform[mois1]": str(yesterday.month),
	"jform[annee1]": str(yesterday.year),
	"jform[jour2]": str(today.day),
	"jform[mois2]": str(today.month),
	"jform[annee2]": str(today.year),
	"jform[start]": "1",
	"jform[export]": "1",
	"submit": "View",
	"option": "com_transparence",
	"view": "recherches",
	"11c051d82963139790cb9094fb16e967": "1"
    }
    
    response = requests.post(excel_url, headers=headers, data=payload)
    #df = pd.read_excel(response.text)
    with io.BytesIO(response.content) as fh:
        df = pd.io.excel.read_excel(fh, skiprows=range(0, 3), usecols='B:E')

    temp = data_supply['LNG']['Montoir (FRA)']
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Nomination'] = float(str(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Allocated Quant'] = float(str(df[df['Day'] == str(datetime.strptime(str(yesterday), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['LNG inventory at the beginning of the gas day'].values[0]*6.666
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Nomination'] = float(str(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated nominated quantities'].values[0]).replace(" ", ""))*(10**-6)
    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Allocated Quant'] = float(str(df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0]).replace(" ", ""))*(10**-6) if df[df['Day'] == str(datetime.strptime(str(today), '%Y-%m-%d').strftime('%d/%m/%Y')) ]['Aggregated allocated quantities'].values[0] == "NaN" else np.NaN
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Montoir (FRA)'] = temp.values

    return data_supply

def terminallng(yesterday, today, data_supply):
    day_from = yesterday.strftime("%Y%m%d")
    day_to = today.strftime("%Y%m%d")
    url_nom = 'https://swi.gaz-system.pl/swi/public/api/lngCapacities?cacheBuster=1642510030891&allItems=false&columns=&count=25&filtering%5BgasDayEnd%5D=ge'+str(day_to)+'&filtering%5BgasDayStart%5D=ge'+str(day_from)+'&lang=en&page=0&sorting=dgasDayStart'
    url_flows = 'https://swi.gaz-system.pl/swi/public/api/actualQuantity?cacheBuster=1643027446612&allItems=false&columns=&count=25&filtering%5Bday%5D=ge'+str(day_from)+',le'+str(day_to)+'&lang=en&operator=PLNG&page=0&sorting=dday,aid'

    response_nom = requests.get(url_nom)
    data_nom = response_nom.json()

    response_flows = requests.get(url_flows)
    data_flows = response_flows.json()

    temp = data_supply['LNG']['Swinoujscie (PL)']

    for dat_flows in data_flows['items']:
        temp.loc[temp.index.get_level_values(0) == dat_flows['day'], temp.columns.get_level_values(0) == 'flow'] = int(dat_flows['allocationE'].replace(" ", ""))*(10**-6)
    for dat_nom in data_nom['items']:
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat_nom['gasDayStart'], '%Y-%m-%d %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Gas in Storage'] = int(round(float(dat_nom['inventory'].replace(" ", "").replace(",","."))))
        temp.loc[temp.index.get_level_values(0) == str(datetime.strptime(dat_nom['gasDayStart'], '%Y-%m-%d %H:%M').strftime('%Y-%m-%d')), temp.columns.get_level_values(0) == 'Nom/renom'] = int(round(float(dat_nom['sendOut'].replace(" ", "").replace(",","."))))
    
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Swinoujscie (PL)'] = temp.values

    return data_supply


def UK_LNG(yesterday, today, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context
    
    options = Options()
    options.set_preference("browser.download.folderList",2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", os.getcwd())
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel")
    options.headless = True
    
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(2)
    driver.get('https://www.nationalgrid.com/uk/gas-transmission/data-and-operations/transmission-operational-data')
    driver.implicitly_wait(2)
    if driver.find_elements_by_id('cky-consent'):
        accept = driver.find_element_by_id('cky-btn-accept')
        accept.click()
    driver.find_element_by_link_text('Daily storage and LNG operator information (1)').click()
    driver.find_element_by_partial_link_text('ST').click()
    time.sleep(1)
    driver.quit()

    os.chdir(os.getcwd())
    for file in glob.glob("ST*.xls"):
        file_name = file

    df = pd.read_excel(file_name)

    if os.path.exists(file_name):
        os.remove(file_name)
    temp_hook = data_supply['LNG UK']['South Hook (UK)']
    temp_dragon = data_supply['LNG UK']['Dragon (UK)']
    temp_isle = data_supply['LNG UK']['Isle of Grain (UK)']

    temp_hook.loc[temp_hook.index.get_level_values(0) == str(yesterday), temp_hook.columns.get_level_values(0) == 'Opening stocks'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'South Hook')]['Opening Stock'].values[0]*(10**-6)
    temp_hook.loc[temp_hook.index.get_level_values(0) == str(yesterday), temp_hook.columns.get_level_values(0) == 'outflow'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'South Hook')]['Outflow'].values[0]*(10**-6)

    temp_dragon.loc[temp_dragon.index.get_level_values(0) == str(yesterday), temp_dragon.columns.get_level_values(0) == 'Opening stocks'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'Dragon')]['Opening Stock'].values[0]*(10**-6)
    temp_dragon.loc[temp_dragon.index.get_level_values(0) == str(yesterday), temp_dragon.columns.get_level_values(0) == 'outflow'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'Dragon')]['Outflow'].values[0]*(10**-6)

    temp_isle.loc[temp_isle.index.get_level_values(0) == str(yesterday), temp_isle.columns.get_level_values(0) == 'Opening stocks'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'Isle Of Grain')]['Opening Stock'].values[0]*(10**-6)
    temp_isle.loc[temp_isle.index.get_level_values(0) == str(yesterday), temp_isle.columns.get_level_values(0) == 'outflow'] = df[(df['Operator Type'] == 'LNG') & (df['Site Name'] == 'Isle Of Grain')]['Outflow'].values[0]*(10**-6)

    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'South Hook (UK)'] = temp_hook.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Dragon (UK)'] = temp_dragon.values
    data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Isle of Grain (UK)'] = temp_isle.values

    return data_supply

def panigaglia(yesyesterday, yesterday, today, data_supply):
    ssl._create_default_https_context = ssl._create_unverified_context

    options = Options()
    options.set_preference("browser.download.folderList",2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", os.getcwd())
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel")
    options.headless = True
    
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(2)
    driver.get('https://www.snam.it/en/transportation/operational-data-business/4-LNG-operational-data/')
    driver.implicitly_wait(2)
    WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.CLASS_NAME, 'selectAllCheckbox'))).click()
    WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.CLASS_NAME, 'btn--category'))).click()
    driver.quit()

    with zipfile.ZipFile('archive.zip') as myzipfile:
        sheet_name = str(yesterday.strftime("%B %Y"))
        myzipfile.extractall()
        myzipfile.close()
        if os.path.exists('gnlitalia-EN.xlsx'):
                to_skip = list(range(1,3))
                for i in range(31, 37):
                    to_skip.append(i)
                df = pd.read_excel('gnlitalia-EN.xlsx', sheet_name=sheet_name, skiprows=to_skip, index_col=0)

                temp = data_supply['LNG']['Panigaglia (ITA)']

                if today.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 2'][today.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 7'][today.day]*(10**-6)
                
                if today.day == 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 2'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 7'][yesterday.day]*(10**-6)

                elif today.day > 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 2'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 7'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 7'][yesyesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 2'][yesyesterday.day]*(10**-6)

                data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Panigaglia (ITA)'] = temp.values
                os.remove('gnlitalia-EN.xlsx')
        if os.path.exists('adriaticlng-en.xlsx'):
                to_skip = list(range(1,3))
                for i in range(31, 37):
                    to_skip.append(i)
                df = pd.read_excel('adriaticlng-en.xlsx', sheet_name=sheet_name, skiprows=to_skip, index_col=0)

                temp = data_supply['LNG']['Carvarzere (ITA)']

                if today.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 8'][today.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 2'][today.day]*(10**-6)

                if today.day == 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 8'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 2'][yesterday.day]*(10**-6)

                if today.day > 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 8'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 2'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 2'][yesyesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 8'][yesyesterday.day]*(10**-6)

                data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Carvarzere (ITA)'] = temp.values
                os.remove('adriaticlng-en.xlsx')
        if os.path.exists('oltlng-en.xlsx'):
                df = pd.read_excel('oltlng-en.xlsx', sheet_name=sheet_name, skiprows=range(1, 6))
                df = df.drop('Unnamed: 0', axis='columns')
                df = df.drop(range(31,38))
                df = df.set_index('Unnamed: 1')

                temp = data_supply['LNG']['Livorno (ITA)']
                
                if today.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 9'][today.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(today), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 3'][today.day]*(10**-6)

                if today.day == 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 9'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 3'][yesterday.day]*(10**-6)
                elif today.day > 2 and yesterday.day in df.index:
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 9'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 3'][yesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'LNG Inventory'] = df['Unnamed: 9'][yesyesterday.day]*(10**-6)
                    temp.loc[temp.index.get_level_values(0) == str(yesyesterday), temp.columns.get_level_values(0) == 'Flow'] = df['Unnamed: 3'][yesyesterday.day]*(10**-6)


                data_supply.loc[data_supply.index.get_level_values(0), data_supply.columns.get_level_values(1) == 'Livorno (ITA)'] = temp.values
                os.remove('oltlng-en.xlsx')
    os.remove('archive.zip')

    return data_supply



def get_data(file_name):
    today = date.today()
    tomorrow = today + timedelta(days = 1)
    yesterday = today - timedelta(days = 1)
    yesyesterday = today - timedelta(days = 2)
    
    df = pd.read_excel(
        file_name,
        index_col = [0],
        header = [0, 1, 2]
    )
    print('working on panigaglia...')
    df = panigaglia(yesyesterday, yesterday, today, df)
    print('panigaglia done')
    print('working on fluxis..')
    df = fluxys(yesyesterday, yesterday, today, df)
    print('fluxis done')

    print('working on uk_lng..')
    df = UK_LNG(yesterday, today, df)
    print('uk_lng done')

    print('working on montior..')
    df = Montoir(yesterday, today, df)
    print('montior done')

    print('working on fos tonkin..')
    df = Fos_Tonkin(yesterday, today, df)
    print('fos_tonkin done')

    print('working on terminallngs..')
    df = terminallng(yesterday, today, df)
    print('terminallng done')

    print('working fosmax..')
    df = fosmax(yesterday, today, df)
    print('fosmax done')

    print('working on grtgaz..')
    df = grtgaz(yesyesterday, yesterday, today, df)
    print('grtgaz')

    print('working on gateterminal..')
    df = gateterminal(yesterday, df)
    print('gateterminal done')

    print('working on snam..')
    df = snam(today, df)
    print('snam done')

    print('working on enagas..')
    df = enagas(yesterday, df)
    print('enagas done')
    

    #NOR
    print('working on fluxis..')
    df = gassco(today, df)

    #Rusko
    print('working on NEL..')
    df = NEL(yesyesterday, tomorrow, df)
    print('NEL done')

    print('working on OPAL..')
    df = OPAL(yesterday, tomorrow, df)
    print('OPAL done')

    print('working on VIP_Bereg_HU_UA..')
    df = VIP_Bereg_HU_UA(yesterday, tomorrow, df)
    print('VIP_Bereg_HU_UA done ')

    print('working on VIP_Bereg_UA_HU..')
    df = VIP_Bereg_UA_HU(yesterday, tomorrow, df)
    print('VIP_Bereg_UA_HU done')

    print('working on jamal_kondratki..')
    df = jamal_kondratki(yesyesterday, tomorrow, df)
    print('jamal_kondratki done')

    print('working on Jamal_Mallnow..')
    df = Jamal_Mallnow(yesyesterday, tomorrow, df)
    print('Jamal_Mallnow done')

    df.to_excel(file_name)
    df = snam(today, df)

    print('done')

def run_scraper():
    #casovac na spustenie scrapera
    runtime = e2.get()
    schedule.every().day.at(runtime).do(get_data, file_name=e1.get())
    
    
    #cyklus na nekonecny beh programu
    while True:
        schedule.run_pending()
        time.sleep(1)
  
if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    os.chdir(os.getcwd())

    master = tk.Tk()
    tk.Label(master, 
            text="File Name").grid(row=0)
    tk.Label(master, 
            text="Interval Time").grid(row=1)

    e1 = tk.Entry(master)
    e2 = tk.Entry(master)

    e1.grid(row=0, column=1)
    e2.grid(row=1, column=1)

    tk.Button(master, 
            text='Quit', 
            command=master.quit).grid(row=3, 
                                        column=0, 
                                        sticky=tk.W, 
                                        pady=4)
    tk.Button(master, 
            text='Start', command=run_scraper).grid(row=3, 
                                                        column=1, 
                                                        sticky=tk.W, 
                                                        pady=4)

    tk.mainloop()