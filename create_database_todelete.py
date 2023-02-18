import psycopg2
from psycopg2.extensions import AsIs

def add_data(dbanme: str, dictonary: dict):
    insert_statement = "INSERT INTO (%s) (%s) values %s"
    cursor.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

conn = psycopg2.connect(
   dbname="predictiondb", user='postgres', password='postgrepassword', 
   host='127.0.0.1', port= '5432'
)
conn.autocommit = True

cursor = conn.cursor()


print("Database created successfully........")
conn.close()
