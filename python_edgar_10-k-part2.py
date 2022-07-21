import requests
import re
import pandas
import sqlite3
from sqlalchemy import create_engine


headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44'
}

con = sqlite3.connect('idx2.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS idx2')
cur.execute(
    'CREATE TABLE idx2 (conm TEXT, type TEXT, cik TEXT, date TEXT, path TEXT)')



engine = create_engine('sqlite:///idx1.db')
with engine.connect() as conn, conn.begin():
    idx = pandas.read_sql_table('idx1', conn)


for index,line in idx.iterrows():

    fn1 = str(line[0])
    fn2 = re.sub(r'[/\\]', '', str(line[1]))
    fn3 = re.sub(r'[/\\]', '', str(line[2]))
    fn4 = str(line[3])
    saveas = '-'.join([fn1, fn2, fn3, fn4])
    url = line[4].strip()
    text=requests.get('%s' % url,headers=headers).text
    found= re.findall(r'"/(.*?txt)"',text)
    path='https://www.sec.gov/'+ found[0]
    print(path, 'downloaded and wrote to SQLite')
    cur.executemany('INSERT INTO idx2 VALUES (?, ?, ?, ?, ?)', [(line[0], line[1], line[2], line[3], path)])
    
con.commit()
con.close()


