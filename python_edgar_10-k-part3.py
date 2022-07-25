import requests
import re
import pandas
import sqlite3
from sqlalchemy import create_engine

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44'
}

con = sqlite3.connect('idx3.db')
cur = con.cursor()
cur.execute(
    'CREATE TABLE if not exists idx3(id TEXT,conm TEXT, type TEXT, cik TEXT, date TEXT, data TEXT)')


engine = create_engine('sqlite:///idx2.db')
with engine.connect() as conn, conn.begin():
    idx = pandas.read_sql_table('idx2', conn)

try:
    engine = create_engine('sqlite:///idx3.db')
    with engine.connect() as conn, conn.begin():
        idx3 = pandas.read_sql_table('idx3', conn)
except:
    idx3 = pandas.DataFrame(columns=['id','conm', 'type', 'cik', 'date', 'path'])

idx_diff=idx[~idx.id.isin(idx3.id)]

for index,line in idx_diff.iterrows():
    fn1 = str(line[1])
    fn2 = re.sub(r'[/\\]', '', str(line[2]))
    fn3 = re.sub(r'[/\\]', '', str(line[3]))
    fn4 = str(line[4])
    url = line[5].strip()
    data=requests.get('%s' % url,headers=headers).text
    print(url, 'downloaded and wrote to SQLite')
    cur.executemany('INSERT INTO idx3 VALUES (?,?, ?, ?, ?, ?)', [(line[0], line[1], line[2], line[3],line[4], data)])
    con.commit()

    
con.close()


