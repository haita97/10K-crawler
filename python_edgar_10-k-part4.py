import requests
import re
import pandas
import sqlite3
from sqlalchemy import create_engine

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44'
}

con = sqlite3.connect('result.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS result')
cur.execute(
    'CREATE TABLE result (conm TEXT, type TEXT, cik TEXT, date TEXT, number TEXT)')

engine = create_engine('sqlite:///idx3.db')
with engine.connect() as conn, conn.begin():
    idx = pandas.read_sql_table('idx3', conn)


pattern=r'supply chain financing|supply chain finance|reverse factoring|supplier finance|supplier financing|structured payable transaction|structured payable|reverse factoring|dynamic discounting|supplier inventory financing'

for index,line in idx.iterrows():
    fn1 = str(line[0])
    fn2 = re.sub(r'[/\\]', '', str(line[1]))
    fn3 = re.sub(r'[/\\]', '', str(line[2]))
    fn4 = str(line[3])
    text = line[4]
    number=0
    result = re.search(pattern, text,re.IGNORECASE)
    if result:
        numebr=len(result)
    cur.executemany('INSERT INTO result VALUES (?, ?, ?, ?, ?)', [(line[0], line[1], line[2], line[3], number)])
    
con.commit()
con.close()


engine = create_engine('sqlite:///result.db')
with engine.connect() as conn, conn.begin():
    result = pandas.read_sql_table('result', conn)
    result.to_csv('result.csv', index=False)