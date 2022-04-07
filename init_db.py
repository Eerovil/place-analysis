
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user="postgres",
    password="mysecretpassword"
)

conn.autocommit = True
cur = conn.cursor()
cur.execute('DROP DATABASE place')
cur.execute('CREATE DATABASE place')

conn.close()
conn = psycopg2.connect(
    host="localhost",
    database="place",
    port="5432",
    user="postgres",
    password="mysecretpassword"
)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS place_users CASCADE')

cur.execute("""
CREATE TABLE place_users(
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR(90) UNIQUE NOT NULL
    )
""")


cur.execute('DROP TABLE IF EXISTS points')
cur.execute("""
CREATE TABLE points (
    time TIMESTAMP NOT NULL,
    coord_x SMALLINT NOT NULL,
    coord_y SMALLINT NOT NULL,
    color VARCHAR(10) NOT NULL,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id)
        REFERENCES place_users (user_id)
        ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (time, user_id)
    )

""")


cur.execute("""
    CREATE INDEX coord_index
    ON points (coord_x, coord_y);
""")

cur.execute("""
    CREATE INDEX time_index
    ON points (time);
""")

conn.commit()

conn.close()
