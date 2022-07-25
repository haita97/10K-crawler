import pandas
import sqlite3
from sqlalchemy import create_engine



engine = create_engine('sqlite:///idx3.db')
with engine.connect() as conn, conn.begin():
    result = pandas.read_sql_table('idx3', conn)
    result.to_excel('idx3.xlsx', index=False)