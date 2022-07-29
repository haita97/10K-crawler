from tkinter import E
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
cur.execute(
    'CREATE TABLE if not exists result (conm TEXT, type TEXT, cik TEXT, date TEXT, number TEXT)')

pattern=r'supply chain financing|supply chain finance|reverse factoring|supplier finance|supplier financing|structured payable transaction|structured payable|reverse factoring|dynamic discounting|supplier inventory financing'



engine = create_engine('sqlite:///idx3.db')
with engine.connect() as conn, conn.begin():
    idx = pandas.read_sql_table('idx3', conn,chunksize=1000)

    for chunk in idx:
        for index,line in chunk.iterrows():
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


