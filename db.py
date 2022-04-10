import psycopg
import time
import datetime


BEGINNING_TIME = datetime.datetime.fromtimestamp(1648817027.221)
END_TIME = datetime.datetime.fromtimestamp(1649112287.221)


def get_conn():
    return psycopg.connect(
        host="localhost",
        dbname="place",
        port="5432",
        user="postgres",
        password="mysecretpassword"
    )


def init_db(conn, users=False):
    """
    point.time is ms after BEGINNING_TIME
    """
    cur = conn.cursor()

    if users:
        cur.execute('DROP TABLE IF EXISTS place_users CASCADE')

    cur.execute("""
    CREATE TABLE IF NOT EXISTS place_users (
        user_name VARCHAR(90) PRIMARY KEY,
        user_id SERIAL
        );
    """)

    cur.execute('DROP TABLE IF EXISTS points')
    cur.execute("""
    CREATE TABLE points (
        time INTEGER NOT NULL,
        coord_x SMALLINT NOT NULL,
        coord_y SMALLINT NOT NULL,
        color INTEGER NOT NULL,
        user_id INTEGER NOT NULL
        )
    """)

    cur.execute("""
        CREATE INDEX time_index
        ON points (time);
    """)

    conn.commit()