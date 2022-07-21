# %%

# Generate the list of index files archived in EDGAR since start_year (earliest: 1993) until the most recent quarter
from sqlalchemy import create_engine
import pandas
import requests
import sqlite3
import datetime
from email import header
# %%
current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
start_year = 2019
years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
history = [(y, q) for y in years for q in quarters]
for i in range(1, current_quarter + 1):
    history.append((current_year, 'QTR%d' % i))
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/crawler.idx' %
        (x[0], x[1]) for x in history]
urls.sort()

# Download index files and write content into SQLite

con = sqlite3.connect('idx1.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS idx1')
cur.execute(
    'CREATE TABLE idx1 (conm TEXT, type TEXT, cik TEXT, date TEXT, path TEXT)')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44'
}


for url in urls:
    lines = requests.get(url, headers=headers).text.splitlines()
    nameloc = lines[7].find('Company Name')
    typeloc = lines[7].find('Form Type')
    cikloc = lines[7].find('CIK')
    dateloc = lines[7].find('Date Filed')
    urlloc = lines[7].find('URL')
    records = [tuple([line[:typeloc].strip(), line[typeloc:cikloc].strip(), line[cikloc:dateloc].strip(),
                      line[dateloc:urlloc].strip(), line[urlloc:].strip()]) for line in lines[9:]]
    cur.executemany('INSERT INTO idx1 VALUES (?, ?, ?, ?, ?)', records)
    print(url, 'downloaded and wrote to SQLite')

con.commit()
con.close()
