
import db
import psycopg

conn = psycopg.connect(
    host="localhost",
    port="5432",
    user="postgres",
    password="mysecretpassword"
)

conn.autocommit = True
cur = conn.cursor()
cur.execute('DROP DATABASE IF EXISTS place')
cur.execute('CREATE DATABASE place')
conn.commit()

conn = db.get_conn()

db.init_db(conn)

conn.close()
