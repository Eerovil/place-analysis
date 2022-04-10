
import db
import psycopg

conn = psycopg.connect(
    host="localhost",
    port="5432",
    user="postgres",
    password="mysecretpassword"
)

conn.autocommit = True
if input("Really init?") == "y":
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS place')
    cur.execute('CREATE DATABASE place')
    conn.commit()

    conn = db.get_conn()
    if input("init users?") == "y":
        db.init_db(conn, users=True)
    else:
        db.init_db(conn)

conn.close()
