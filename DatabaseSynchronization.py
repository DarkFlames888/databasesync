#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install psycopg2')


# In[36]:


import pyodbc
import psycopg2
import sys


# In[37]:


class DatabaseSync:
    def __init__(self):
        self.conn1 = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                    server=sys.argv[1],
                                    database=sys.argv[2])
        self.conn2 = psycopg2.connect(database=sys.argv[3], host=sys.argv[4], user=sys.argv[5],password=sys.argv[6])
        self.cur1 = self.conn1.cursor()
        self.cur2 = self.conn2.cursor()
    def from_sql_db(self):
        self.cur1.execute('SELECT col1, col2 FROM src_table WHERE 1=1')
        rows_cur = self.cur1.fetchall()
        print('Total Records to be synced:', len(rows_cur))
        return rows_cur
    def upsert_to_postgres(self, data):
        cnt_insert = 0
        cnt_update = 0
        for row in data:
            query = 'SELECT count(*) FROM target_table WHERE 1=1'
            self.cur2.execute(query)
            count = self.cur2.fetchone()[0]
            if count == 0:
                query = 'INSERT INTO target_table(col1,col2) VALUES (%s,%s)'
                self.cur2.execute(query, data[row])
                count_insert = cnt_insert + 1
            else:
                query = 'UPDATE target_table SET col1 = %s, col2 = %s WHERE 1=1'
                self.cur2.execute(query, data[row])
                count_update = cnt_update + 1
                self.conn1.commit()
                self.conn2.commit()
                print('Number of records inserted:', cnt_insert)
                print('Number of records updated:', cnt_update)


# In[38]:


#Pass the database parameters here source and target
inst = DatabaseSync()
inst.upsert_to_postgres(inst.from_sql_db())
#exexute the script using the below command
#./python3 DatabaseSynchronization.py "192.168.1.7" "srcdb" "dev" "12345" "tgtdb" "192.168.1.7" "tgtuser" "!@#$%"

