"""
Original file is located at
    https://colab.research.google.com/drive/1_UrD_Do_hziXHXMPTJYDZjFSEzQEwmuY
"""

!pip install pymongo
!pip install tabula-py

# Commented out IPython magic to ensure Python compatibility.
# # Set up for running selenium in Google Colab
# ## You don't need to run this code if you do it in Jupyter notebook, or other local Python setting
%%shell
!sudo apt -y update
!sudo apt install -y wget curl unzip
!wget http://archive.ubuntu.com/ubuntu/pool/main/libu/libu2f-host/libu2f-udev_1.1.4-1_all.deb
!dpkg -i libu2f-udev_1.1.4-1_all.deb
!wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
!dpkg -i google-chrome-stable_current_amd64.deb
!CHROME_DRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`
!wget -N https://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip -P /tmp/
!unzip -o /tmp/chromedriver_linux64.zip -d /tmp/
!chmod +x /tmp/chromedriver
!mv /tmp/chromedriver /usr/local/bin/chromedriver
!pip install selenium

!pip install chromedriver-autoinstaller
import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
chrome_options = webdriver.ChromeOptions()  # setup chrome options
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')  # ensure GUI is off
chrome_options.add_argument('--disable-dev-shm-usage')
chromedriver_autoinstaller.install()    # set path to chromedriver as per your configuration
driver = webdriver.Chrome(options=chrome_options)   # set up the webdriver

import pandas as pd
import requests
import re
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import pymongo
import tabula
import json
import csv
import ssl
import warnings
import urllib3
import random
from bs4 import BeautifulSoup
ssl._create_default_https_context = ssl._create_unverified_context

"""## Defining important functions"""

user_agents_list = [
    'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
]

warnings.filterwarnings("ignore")

# removing non numeric characters from a string
def remove_non_numeric_chars(input_string):
    return float(re.sub('[^0-9.]', '', input_string))

# removing words bewteen parentheses and convert all letters to small
def words_bw_parentheses(input_string):
    input_string.lower()
    pattern = r'\([^()]*\)'
    return re.sub(pattern, '', input_string)

# removing special characters
def remove_special_characters(df, column):
    original_types = df[column].dtypes
    df[column] = df[column].astype(str).apply(lambda x: re.sub(r'[^\w\s]', '', x))
    df[column] = df[column].astype(original_types)
    return df

# converting into words into date
def to_date(input_day):
    input_day = input_day.lower()
    words = input_day.split()

    data = {}
    adjust_days = 0

    for i, word in enumerate(words):
        if word == 'year' or word == 'years':
            data['years'] = int(words[i - 1])
        elif word == 'month' or word == 'months':
            data['months'] = int(words[i - 1])
        elif word == 'day' or word == 'days':
            data['days'] = int(words[i - 1])
        elif word == 'less' and words[i + 1] == 'than':
            adjust_days = -1
        elif word == 'above':
            adjust_days = 1

    today = date.today()
    future_date = today + relativedelta(years=data.get('years', 0), months=data.get('months', 0), days=data.get('days', 0))

    if adjust_days != 0:
        future_date += relativedelta(days=adjust_days)

    return future_date

# Extracting tables from pdf
def extract_table_from_pdf(url, page_numbers):
    response = requests.get(url)
    response.raise_for_status()

    with open("temp_pdf.pdf", "wb") as f:
        f.write(response.content)

    tables = []
    for page_number in page_numbers:
        tables.extend(tabula.read_pdf("temp_pdf.pdf", pages=page_number))

    import os
    os.remove("temp_pdf.pdf")

    return tables

def add_days_after_number(cell):
    if cell.strip().isdigit():
        return cell + ' days'
    return cell

"""## Connecting to MongoDB"""

#PyMongo Client
import pymongo
client = pymongo.MongoClient('mongodb+srv://theanishk:sXDNAjLpOZvPdAQa@fdproject.qwukrev.mongodb.net/?retryWrites=true&w=majority')
db = client['FD_project']
collection = db['interest_rate']

"""### Deleting all documents (Data) from database"""

# result = collection.delete_many({})

"""### Retriving all documents from database"""

# Retrieve all documents from the collection
documents = collection.find()

# Iterate over the documents and display their contents
for doc in documents:
    print(doc)

"""# Collecting Bank's IR Data

###Bank of Baroda
"""

def bob():
    requests.packages.urllib3.disable_warnings()
    url = 'https://www.bankofbaroda.in/interest-rate-and-service-charges/deposits-interest-rates'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df = tables[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace(' & above', '').str.replace('upto', '').str.replace('and', 'to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df = df[~df['Maturity'].str.contains('Above 10 years')]
        return df

    except:
        print("Error Encountered")

try:
    df = bob()

    result = collection.delete_one({'bank': "Bank of Baroda"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Bank of Baroda",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Bank of India"""

def boi():
    requests.packages.urllib3.disable_warnings()
    url='https://bankofindia.co.in/interest-rate/rupee-term-deposit-rate'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.columns=['Maturity','General Public','Senior Citizen']
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('& above', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = boi()

    result = collection.delete_one({'bank': "Bank of India"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Bank of India",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Bank of Maharashtra"""

def bom():
    requests.packages.urllib3.disable_warnings()
    url="https://bankofmaharashtra.in/domestic-term-deposits"
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.columns = ['Maturity', 'General Public', 'waste1', 'waste2']
        df.drop(columns = ['waste1', 'waste2'], inplace=True)
        df.drop((df['Maturity'] == 'Duration').index[0], inplace=True)
        df = df[~df['Maturity'].str.startswith('Special')]
        df.loc[df['Maturity'] == '365 days/ One Year', 'Maturity'] = '1 Year'
        df.loc[df['Maturity'] == 'Above 5 years', 'Maturity'] = 'Above 5 years-10 years'
        df['Senior Citizen'] = df['General Public']
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        senior_start = df[df.apply(lambda row: row.astype(str).str.contains('91')).any(axis=1)].index[0]
        df.loc[senior_start:, 'Senior Citizen'] = (df.loc[senior_start:, 'General Public'].astype(float) + 0.50).astype(str)
        df[['min', 'max']] = df['Maturity'].str.replace('Over','Above').str.replace('to','-').str.split('-', expand=True)
        df['min'] = df['min'].apply(add_days_after_number)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df = remove_special_characters(df, 'max')
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = bom()

    result = collection.delete_one({'bank': "Bank of Maharashtra"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Bank of Maharashtra",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Canara Bank (Numbers)"""

def canara():
    requests.packages.urllib3.disable_warnings()
    url = 'https://canarabank.com/User_page.aspx?othlink=9'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[1]
        df.drop(df.index[:6], inplace=True)
        df.drop(columns = df.columns[5:], inplace=True)
        df.columns = ['Maturity', 'General Public', 'GPAY', 'Senior Citizen', 'SCAY']
        df.drop(columns = ['GPAY', 'SCAY'], inplace=True)
        df = df[~df['Maturity'].str.startswith('$-Non-callable')]
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('& above', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = canara()

    result = collection.delete_one({'bank': "Canara Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Canara Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Central Bank of India"""

def cbi():
    requests.packages.urllib3.disable_warnings()
    url='https://centralbankofindia.co.in/en/interest-rates-on-deposit'
    try:
        response = requests.get(url, verify=False)
        tables = pd.read_html(response.content)
        df=tables[1].drop([0,1], axis=0)
        df.dropna(inplace=True)
        df.columns = ['Maturity', 'General Public', 'GPAY', 'Senior Citizen', 'SCAY']
        df.drop(columns = ['GPAY', 'SCAY'], inplace=True)
        df.loc[df['Maturity'] == '7 - l4 days', 'Maturity'] = '7 -14 days'
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace(' & above', '').str.replace('yr', 'year').str.replace('upto', '-').str.replace('to', '-').str.split('-', expand=True)
        df['min'] = df['min'].apply(add_days_after_number)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = cbi()

    result = collection.delete_one({'bank': "Central Bank of India"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Central Bank of India",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Indian Bank"""

def ib():
    requests.packages.urllib3.disable_warnings()
    url = 'https://www.indianbank.in/departments/deposit-rates/#!'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df = tables[0]
        contains_existing = df.apply(lambda col: col.astype(str).str.startswith('Existing')).any()
        df = df.loc[:, ~contains_existing]
        df.columns = ['Maturity', 'General Public']
        df = df[~df['Maturity'].str.contains('Period')]
        df['Senior Citizen'] = df['General Public']
        senior_start = df[df.apply(lambda row: row.astype(str).str.contains('15')).any(axis=1)].index[0]
        df.loc[senior_start:, 'Senior Citizen'] = (df.loc[senior_start:, 'General Public'].astype(float) + 0.50).astype(str)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df.loc[df['Maturity'] == 'Above 5 years', 'Maturity'] = 'Above 5 years to 10 years'
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = ib()

    result = collection.delete_one({'bank': "Indian Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Indian Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Indian Overseas Bank"""

def iob():
    requests.packages.urllib3.disable_warnings()
    url='https://www.iob.in/Domestic_Rates'
    try:
        response = requests.get(url, verify=False)
        tables = pd.read_html(response.content)
        df=tables[0]
        df = df.filter(regex='^(?!.*Existing).*')
        df.columns = ['Maturity', 'General Public']
        df['Senior Citizen'] = (df['General Public'] + 0.5).astype(str)
        df.loc[df['Maturity'] == '3 Years and Above', 'Maturity'] = 'Above 3 years to 10 years'
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('to', '-').str.replace('<', 'less than').str.split('-', expand=True)
        df['min'] = df['min'].apply(add_days_after_number)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = iob()

    result = collection.delete_one({'bank': "Indian Overseas Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Indian Overseas Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Punjab and Sind Bank"""

def pns():
    requests.packages.urllib3.disable_warnings()
    url='https://punjabandsindbank.co.in/content/interestdom'
    try:
        response = requests.get(url, verify=False)
        tables = pd.read_html(response.content)
        df = tables[1]
        df.columns = ['Maturity', 'General Public']
        df = df[~df['Maturity'].str.contains('Maturity')]
        df['General Public'] = df['General Public'].apply(words_bw_parentheses)
        df['Senior Citizen'] = df['General Public']
        senior_start = df[df.apply(lambda row: row.astype(str).str.contains('180')).any(axis=1)].index[0]
        df.loc[senior_start:, 'Senior Citizen'] = (df.loc[senior_start:, 'General Public'].astype(float) + 0.50).astype(str)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('-<','to less than ').str.replace('>', 'Above ').str.replace('<','less than ').str.replace('–','-').str.replace('Yearsto',' Years to ').str.replace('Above','Above ').str.replace('Year',' Year').str.replace('Day',' Day').str.replace('to','-').str.split('-', expand=True)
        df['min'] = df['min'].apply(add_days_after_number)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))

        return df
    except:
        print("Error Encountered")

try:
    df = pns()

    result = collection.delete_one({'bank': "Punjab and Sind Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Punjab and Sind Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Punjab National Bank"""

def pnb():
    requests.packages.urllib3.disable_warnings()
    url='https://www.pnbindia.in/interest-rates-deposit.html'
    try:
        response = requests.get(url, verify=False)
        tables = pd.read_html(response.content)
        df=tables[16]
        df.columns = df.iloc[0]
        df = df.filter(regex='^(?!.*Existing).*')
        df.drop(columns=[df.columns[-1]], inplace=True)
        df.drop(columns=[df.columns[0]], inplace=True)
        df.drop(0, inplace=True)
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('& upto', 'to').str.replace('s to', '-').str.replace('r to', 'r -').str.replace('rs to', 'r -').str.replace('to', 'Days -').str.replace('>1Year', 'less than 1 Year').str.replace('<', 'less than').str.split('-', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = pnb()

    result = collection.delete_one({'bank': "Punjab National Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Punjab National Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### State Bank of India"""

def sbi():
    requests.packages.urllib3.disable_warnings()
    url = 'https://sbi.co.in/web/interest-rates/deposit-rates/retail-domestic-term-deposits'
    try:
        response = requests.get(url, verify=False)
        tables = pd.read_html(response.content)
        df = tables[0]
        df.dropna(inplace = True)
        df.columns = ['Maturity', 'General Public', 'GPAY', 'Senior Citizen', 'SCAY']
        df.drop(columns=['GPAY', 'SCAY'], inplace=True)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df.Maturity.str.replace('and', '').str.replace('up', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = sbi()

    result = collection.delete_one({'bank': "State Bank of India"})

    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "State Bank of India",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### UCO Bank"""

def uco():
    requests.packages.urllib3.disable_warnings()
    url='https://www.ucobank.com/English/interest-rate-deposit-account.aspx'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[1]
        df.columns = ['Maturity', 'General Public', 'GPAY', 'Senior Citizen', 'SCAY']
        df.drop(columns = ['GPAY', 'SCAY'], inplace=True)
        df = df[~df['Maturity'].str.startswith('Maturity')]
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df.loc[df['Maturity'] == '445 -2 Yrs', 'Maturity'] = '445 days - 2 Yrs'
        df.loc[df['Maturity'] == 'Above 5 yrs', 'Maturity'] = 'Above 5 years to 10 years'
        df[['min', 'max']] = df['Maturity'].str.replace('Yrs','yrs').str.replace('Yr','yrs').str.replace('yrs',' years').str.replace(' -',' to ').str.replace('-',' days to ').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = uco()

    result = collection.delete_one({'bank': "UCO Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "UCO Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Union Bank of India"""

def ubi():
    url='https://www.unionbankofindia.co.in/english/interest-rate.aspx'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[1].drop(range(0,4), axis=0)
        df.columns=['Maturity','General Public']
        waste_start = df[df.apply(lambda row: row.astype(str).str.contains('Union Bank of India')).any(axis=1)].index[0]
        df.drop(index=df.loc[waste_start:].index, inplace=True)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['General Public'].astype(float)+0.50
        df[['min', 'max']] = df['Maturity'].str.replace('-', ' Days to').str.replace('<', 'less than').str.replace('>', 'Above ').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = ubi()

    result = collection.delete_one({'bank': "Union Bank of India"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Union Bank of India",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Axis Bank

https://www.axisbank.com/retail/deposits/fixed-deposits/index?cta=homepage-lhs-open-fd
"""

def axis():
    url = "https://www.axisbank.com/interest-rate-on-deposits"
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find("a", class_="FDlink")
        link = 'https://www.axisbank.com'+links["href"]
        tables = extract_table_from_pdf(link, [1, 2]) # page 1 and page 2
        tables[0].columns = ['SNo', 'Maturity', 'General Public', 'waste1', 'waste2', 'waste3']
        tables[0].drop(columns=['SNo', 'waste1', 'waste2', 'waste3'], inplace=True)
        tables[0].dropna(inplace=True)
        tables[1].columns = ['SNo', 'Maturity', 'Senior Citizen', 'waste1', 'waste2', 'waste3']
        tables[1].drop(columns=['SNo', 'waste1', 'waste2', 'waste3'], inplace=True)
        tables[1].dropna(inplace=True)
        df = pd.concat([tables[0], tables[1].drop(columns=['Maturity'])], axis = 1)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('<', 'to less than').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = axis()

    result = collection.delete_one({'bank': "Axis Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Axis Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Bandhan Bank"""

def bb():
    url='https://bandhanbank.com/rates-charges'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[4]
        df.dropna(inplace=True)
        df.columns=['Maturity','General Public','Senior Citizen']
        df = df[~df['Maturity'].str.startswith('Maturity')]
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('up to', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = bb()

    result = collection.delete_one({'bank': "Bandhan Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Bandhan Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### CSB Bank"""

def csb():
    url ='https://www.csb.co.in/interest-rates'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        tables[0] = tables[0].drop(columns=[tables[0].columns[0]])
        tables[2] = tables[2].drop(columns=[tables[2].columns[0]])
        tables[0].columns = ['Maturity', 'General Public']
        tables[2].columns = ['Maturity', 'Senior Citizen']
        tables[0].drop(tables[0][tables[0]['Maturity'] == 'Deposit Tenor'].index, inplace=True)
        tables[2].drop(tables[2][tables[2]['Maturity'] == 'Deposit Tenor'].index, inplace=True)
        tables[0]['Senior Citizen'] = tables[0]['General Public']
        value_to_search = tables[2]['Maturity'][1]
        common_index = tables[0][tables[0]['Maturity'] == value_to_search].index[0]
        tables[0]['Senior Citizen'][common_index -1:] = tables[2]['Senior Citizen']
        df = tables[0]
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))

        return df

    except:
        print("Error Encountered")

try:
    df = csb()

    result = collection.delete_one({'bank': "CSB Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "CSB Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### City Union Bank (Scraper API)"""

def cub():
    url='http://api.scraperapi.com?api_key=b7ea70db3cfa2495ab015c4d02780f1e&url=https://www.cityunionbank.com/deposit-interest-rate'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.columns=['Maturity', 'General Public', 'Senior Citizen', 'waste']
        df.drop(columns = ['waste'], inplace=True)
        df = df[~df['Maturity'].str.startswith('For *NRO')]
        df.dropna(axis=0, inplace=True)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['min'] = df['min'].apply(add_days_after_number)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = cub()

    result = collection.delete_one({'bank': "City Union Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "City Union Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### DCB Bank"""

def dcb():
    url ='https://www.dcbbank.com/dcb-fixed-deposits/deposit-rates'
    try:
        tables=pd.read_html(url)
        df=tables[2]
        waste_col = df.apply(lambda col: col.astype(str).str.startswith('Effective')).any()
        df = df.loc[:, ~waste_col]
        df.dropna(axis=1, inplace=True)
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df.drop(df[df['Maturity'] == 'Tenure'].index, inplace=True)
        df['General Public']=df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen']=df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('More than', 'above').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))

        return df

    except:
        print("Error Encountered")

try:
    df = dcb()

    result = collection.delete_one({'bank': "DCB Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "DCB Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### Dhanlaxmi Bank"""

def dhan():
    url ='https://www.dhanbank.com/interest-rates/'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[1]
        df.columns = ['Maturity', 'General Public']
        df = df[~df['Maturity'].str.startswith('Term')]
        df = df[~df['General Public'].str.startswith('Term')]
        df = df[~df['General Public'].str.startswith('Rates')]
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df['Senior Citizen'] = df['General Public']
        senior_start = df[df.apply(lambda row: row.astype(str).str.contains('1 Year')).any(axis=1)].index[0]
        df.loc[senior_start:, 'Senior Citizen'] = (df.loc[senior_start:, 'General Public'].astype(float) + 0.50).astype(str)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('one', '1').str.replace('upto & inclusive of', 'to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df=dhan()

    result = collection.delete_one({'bank': "Dhanlaxmi Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Dhanlaxmi Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")

 

"""### Federal Bank"""

def fb():
    url='https://www.federalbank.co.in/deposit-rate'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.columns=['Maturity','General Public','Senior Citizen']
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df.loc[df['Maturity'] == '5 years and above', 'Maturity'] = '5 years to 10 years'
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = fb()

    result = collection.delete_one({'bank': "Federal Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Federal Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### HDFC Bank (Using API)"""

def hdfc():
    url='http://api.scraperapi.com?api_key=b7ea70db3cfa2495ab015c4d02780f1e&url=https://www.hdfcbank.com/personal/save/deposits/fixed-deposit-interest-rate'
    try:
        response = requests.get(url)
        tables = pd.read_html(response.content)
        df = tables[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('<=', 'to').str.replace('< =', 'to').str.replace('<', 'less than').str.replace('s -', 's to').str.replace('y -', 'y to').str.replace('-', 'days to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error encountered")

try:
    df = hdfc()

    result = collection.delete_one({'bank': "HDFC Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "HDFC Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")
 

"""### ICICI Bank"""

def icici():
    url = 'https://www.icicibank.com/personal-banking/deposits/fixed-deposit/fd-interest-rates'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df = tables[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen', 'waste1', 'waste2']
        df.drop(columns=['waste1', 'waste2'], axis=1, inplace=True)
        df.dropna(inplace=True)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df = df[~df['Maturity'].str.contains('(80C FD)')]
        df[['min', 'max']] = df['Maturity'].str.replace('<', 'less than').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = icici()

    result = collection.delete_one({'bank': "ICICI Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "ICICI Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")

 

"""### IndusInd Bank

https://myaccount.indusind.com/fdonline/index.aspx
"""

def iib():
    url = 'https://www.indusind.com/in/en/personal/rates.html#term-deposit-tab'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df = tables[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df.loc[df['Maturity'] == 'Indus Tax Saver Scheme (5 years)', 'Maturity'] = '5 years (Indus Tax Saver Scheme)'
        df.loc[df['Maturity'] == '61 month and above', 'Maturity'] = '61 month to 10 years'
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('below', 'less than').str.replace('up', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = iib()

    result = collection.delete_one({'bank': "IndusInd Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "IndusInd Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")

 

"""### IDFC FIRST Bank"""

def idfc():
    url = 'https://www.idfcfirstbank.com/personal-banking/deposits/fixed-deposit/fd-interest-rates'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df.drop(df[df['Maturity'] == 'Tenure'].index, inplace=True)
        df.loc[df['Maturity'] == '2 years-1 day – 749 days', 'Maturity'] = '2 years 1 day – 749 days'
        df[['min', 'max']] = df['Maturity'].str.replace(u'\xa0', u' ').str.replace('y–', 'y to').str.replace('s –', 's to').str.replace('y –', 'y to').str.replace('y -', 'y to').str.replace('–', 'days to').str.split('to', expand=True)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = idfc()

    result = collection.delete_one({'bank': "IDFC FIRST Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "IDFC FIRST Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")

 

"""### Jammu & Kashmir Bank"""

def jk():
    url='http://www.jkbank.com/others/common/intrates.php'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        soup = BeautifulSoup(response.content, 'html.parser')

        rows = soup.find_all('table')[1].find_all('tr')

        table_data = []

        for row in rows:
            cells = row.find_all('td')
            row_data = [cell.get_text(strip=True) for cell in cells]
            table_data.append(row_data)

        df = pd.DataFrame(table_data[1:], columns=table_data[0])
        df.columns = ['Maturity', 'waste', 'General Public', 'Senior Citizen']
        df.drop(columns = ['waste'], inplace=True)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print("Error Encountered")

try:
    df = jk()

    result = collection.delete_one({'bank': "Jammu & Kashmir Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Jammu & Kashmir Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


 

"""### Karnataka Bank"""

def kb():
    url='https://karnatakabank.com/personal/term-deposits/interest-rates'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df = df[~df[1].str.startswith('Interest')]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen', 'waste']
        df.drop(columns=['waste'], inplace=True)
        df['Senior Citizen'] = df['General Public'].astype(float) + 0.40
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('below', 'less than').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = kb()

    result = collection.delete_one({'bank': "Karnataka Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Karnataka Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")

"""### Karur Vysya Bank (selenium, no senior citizen)"""

def kvb():
    url = 'https://www.kvb.co.in/interest-rates/resident-nro-deposits/'
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        tables = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'table-responsive')))
        table1 = tables[0]
        table2 = tables[1]
        time.sleep(2)
        table1_html = table1.get_attribute('outerHTML')
        table2_html = table2.get_attribute('outerHTML')
        driver.quit()
        table1 = pd.read_html(table1_html)[0]
        table2 = pd.read_html(table2_html)[0]
        table1.columns = ['Maturity', 'General Public', 'waste']
        table2.columns = ['Maturity', 'Senior Citizen', 'waste']
        table1.drop(columns=['waste'], inplace=True)
        table2.drop(columns=['waste'], inplace=True)
        df = table1
        df = df[~df['Maturity'].str.contains('rainbow')]
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('upto', 'to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = kvb()

    result = collection.delete_one({'bank': "Karur Vysya Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Karur Vysya Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Kotak Mahindra Bank"""

def kotak():
    url = 'https://www.kotak.com/bank/mailers/intrates/get_all_variable_data_latest.php?section=NRO_Term_Deposit'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df = tables[0]
        df.drop([0,1,2], axis=0, inplace=True)
        df.dropna(axis=1, inplace=True)
        df.columns = ['Maturity', 'General Public', 'GPAY', 'Senior Citizen', 'SCAY']
        df.drop(columns=['GPAY', 'SCAY'], axis=0, inplace=True)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('Day-', 'Days to').str.replace('years-', 'years to').str.replace('Days -', 'Days to').str.replace('-', 'Days to').str.replace('and above', '').str.replace('but', 'to').str.replace('upto and', 'to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))

        return df

    except:
        print('Error encountered')

try:
    df = kotak()

    result = collection.delete_one({'bank': "Kotak Mahindra Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Kotak Mahindra Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Nainital Bank"""

def nb():
    url ='https://www.nainitalbank.co.in/English/interest_rate.aspx'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[1]
        df.dropna(axis=1, inplace=True)
        df = df[~df[1].str.startswith('w.e.f.')]
        df = df[~df[0].str.startswith('Maturity')]
        df = df[~df[1].str.startswith('Rates')]
        waste_start = df[df.apply(lambda row: row.astype(str).str.startswith('Naini Tax Saver')).any(axis=1)].index[0]
        df.drop(index=df.loc[waste_start:].index, inplace=True)
        df.columns = ['Maturity', 'General Public']
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['General Public'].astype(float) + 0.50
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('and above', '').str.replace(' or equal to', '').str.replace(' upto', '').str.replace('but less than', 'to').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = nb()

    result = collection.delete_one({'bank': "Nainital Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Nainital Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### RBL Bank (selenium)"""

def rbl():
    url='https://www.rblbank.com/interest-rates/fd-rates'
    try:
        driver = webdriver.Chrome(options=chrome_options)   # set up the webdriver
        driver.get(url)
        wait = WebDriverWait(driver, 10)    # Wait for the table to load (adjust the timeout as needed)
        tables = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'tableStriped')))
        table = tables[1]   # second table
        time.sleep(2)   # Wait for a short time to ensure JavaScript rendering is complete
        table_html = table.get_attribute('outerHTML')
        driver.quit()   # Close the browser
        df = pd.read_html(table_html)[0]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen', 'waste1', 'waste2']
        df.drop(columns=['waste1', 'waste2'], inplace=True)
        df.loc[df['Maturity'] == 'Tax Savings Fixed Deposit (60 months)', 'Maturity'] = 'Tax Savings 60 months'
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))

        return df

    except:
        print('Error encountered')

try:
    df = rbl()

    result = collection.delete_one({'bank': "RBL Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "RBL Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### South Indian Bank"""

def sib():
    url='https://www.southindianbank.com/interestrate/interestratelist.aspx'
    try:
        response = requests.get(url, verify=False, headers={'User-Agent': random.choice(user_agents_list)})
        tables = pd.read_html(response.content)
        df=tables[0]
        df.dropna(subset=['Period'], inplace=True)
        df.dropna(axis=1, inplace=True)
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df.loc[df['Maturity'] == 'Tax Gain ( 5 Years )', 'Maturity'] = '5 Years Tax Gain'
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('upto and including', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = sib()

    result = collection.delete_one({'bank': "South Indian Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "South Indian Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### Tamilnad Mercantile Bank"""

def tmb():
    url='https://www.tmb.in/deposit-interest-rates.aspx'
    try:
        tables=pd.read_html(url)
        df=tables[1]
        df.columns = ['Maturity', 'General Public', 'Senior Citizen']
        df = df[~df[df.columns].apply(lambda col: col.str.startswith('RATE')).any(axis=1)]
        df = df[~df[df.columns].apply(lambda col: col.str.startswith('Period')).any(axis=1)]
        df = df[~df[df.columns].apply(lambda col: col.str.startswith('w.e.f')).any(axis=1)]
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df[['min', 'max']] = df['Maturity'].str.replace('-', ' days to ').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = tmb()

    result = collection.delete_one({'bank': "Tamilnad Mercantile Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "Tamilnad Mercantile Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""### YES Bank (Time out)"""



"""### IDBI Bank"""

def idbi():
    url='https://www.idbibank.in/interest-rates.aspx'
    try:
        tables=pd.read_html(url)
        tables[4].drop(columns=['Senior Citizens.1'], inplace=True)
        tables[3].columns = ['Maturity', 'General Public', 'Senior Citizen']
        tables[3].dropna(inplace=True)
        tables[4].columns = ['Maturity', 'General Public', 'Senior Citizen']
        tables[4].dropna(inplace=True)
        tables[3]['General Public'] = tables[3]['General Public'].astype(str)
        tables[3]['Senior Citizen'] = tables[3]['Senior Citizen'].astype(str)
        tables[4]['General Public'] = tables[4]['General Public'].astype(str)
        tables[4]['Senior Citizen'] = tables[4]['Senior Citizen'].astype(str)
        df = pd.concat([tables[3], tables[4]], axis=0)
        df['General Public'] = df['General Public'].apply(remove_non_numeric_chars)
        df['Senior Citizen'] = df['Senior Citizen'].apply(remove_non_numeric_chars)
        df['Maturity'] = df['Maturity'].apply(words_bw_parentheses)
        df[['min', 'max']] = df['Maturity'].str.replace('year', ' year').str.replace('-', ' days to ').str.replace('<', 'less than').str.replace('>', 'above ').str.replace('$', '').str.split('to', expand=True)
        df['max'] = df.apply(lambda row: row['min'] if pd.isna(row['max']) else row['max'], axis=1)
        df['min'] = df['min'].apply(to_date)
        df['max'] = df['max'].apply(to_date)
        df['min'] = df['min'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        df['max'] = df['max'].apply(lambda x: datetime.combine(x, datetime.min.time()))
        return df

    except:
        print('Error encountered')

try:
    df = idbi()

    result = collection.delete_one({'bank': "IDBI Bank"})
    data = df.to_dict(orient='records')

    bank_data = {
        "bank": "IDBI Bank",
        "interest_rates": data
    }

    collection.insert_one(bank_data)
except:
    print("Something went wrong")


"""# Exporting MongoDB documents to CSV"""

today = datetime.date(datetime.today())

# Query all the documents from the collection
data = collection.find({})
data_list = list(data)
df = pd.DataFrame(data_list)
df.drop('_id', axis=1, inplace=True)

# Prepare the data to extract required fields
csv_data = []
for doc in data_list:
    bank_name = doc['bank']
    for interest_rate in doc['interest_rates']:
        maturity = interest_rate['Maturity']
        general_public_rate = interest_rate['General Public']
        senior_citizen_rate = interest_rate['Senior Citizen']
        min = interest_rate['min']
        max = interest_rate['max']
        csv_data.append({'Bank Name': bank_name, 'Maturity': maturity, 'General Public': general_public_rate, 'Senior Citizen': senior_citizen_rate})

# Convert the list of dictionaries to a DataFrame for the new data
df_new = pd.DataFrame(csv_data)

try:
    # Try to read the previous CSV file (if it exists)
    df_prev = pd.read_csv(f'output{today - timedelta(days = 1)}.csv')
except FileNotFoundError:
    df_prev = pd.DataFrame()  # Create an empty DataFrame if the previous CSV file does not exist

# Compare the new DataFrame with the previous DataFrame
if df_new.equals(df_prev):
    print("Data has not changed. No need to save today's CSV.")
else:
    # Export the new DataFrame to a CSV file and overwrite the previous one
    df_new.to_csv(f'output{today}.csv', index=False)
    print("Data has changed. Today's CSV saved.")