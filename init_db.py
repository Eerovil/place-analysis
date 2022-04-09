
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

conn.close()
conn = psycopg.connect(
    host="localhost",
    dbname="place",
    port="5432",
    user="postgres",
    password="mysecretpassword"
)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS place_users CASCADE')

cur.execute('DROP TABLE IF EXISTS points')
cur.execute("""
CREATE TABLE points (
    time INTEGER NOT NULL,
    coord_x SMALLINT NOT NULL,
    coord_y SMALLINT NOT NULL,
    color VARCHAR(10) NOT NULL,
    user_name VARCHAR(90) NOT NULL,
    PRIMARY KEY (time, coord_x, coord_y, user_name)
    )
""")

cur.execute("""
    CREATE INDEX coord_index
    ON points (coord_x, coord_y);
""")

cur.execute("""
    CREATE INDEX user_index
    ON points (user_name);
""")

cur.execute("""
    CREATE INDEX time_index
    ON points (time);
""")

conn.commit()

conn.close()
