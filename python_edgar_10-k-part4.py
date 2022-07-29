import pandas
import sqlite3
from sqlalchemy import create_engine
import os
headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44'
}

con = sqlite3.connect('result.db')
cur = con.cursor()
cur.execute(
    'CREATE TABLE if not exists result (conm TEXT, type TEXT, cik TEXT, date TEXT, number TEXT)')

engine = create_engine('sqlite:///idx3.db')
with engine.connect() as conn, conn.begin():
    idx = pandas.read_sql_table('idx3', conn)

for index,line in idx.iterrows():
    fn1 = str(line[3])
    fn2 = str(line[4])
    name=fn1+'-'+fn2
    if not os.path.exists(name):
        with open('html/'+name+'.html', 'w') as f:
            f.write(line[5])
