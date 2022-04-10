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


def init_db(conn):
    """
    point.time is ms after BEGINNING_TIME
    """
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS place_users CASCADE')

    cur.execute("""
    CREATE TABLE place_users (
        user_id SERIAL PRIMARY KEY,
        user_name VARCHAR(90) NOT NULL
        );
    """)

    cur.execute('DROP TABLE IF EXISTS points')
    cur.execute("""
    CREATE TABLE points (
        time INTEGER NOT NULL,
        coord_x SMALLINT NOT NULL,
        coord_y SMALLINT NOT NULL,
        color INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        FOREIGN KEY (user_id) REFERENCES place_users(user_id),
        PRIMARY KEY (time, coord_x, coord_y, user_id)
        )
    """)

    cur.execute("""
        CREATE INDEX coord_index
        ON points (coord_x, coord_y);
    """)

    cur.execute("""
        CREATE INDEX user_index
        ON place_users (user_name);
    """)

    cur.execute("""
        CREATE INDEX time_index
        ON points (time);
    """)

    conn.commit()