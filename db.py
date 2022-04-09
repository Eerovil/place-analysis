import psycopg
import time

def get_conn():
    return psycopg.connect(
        host="localhost",
        dbname="place",
        port="5432",
        user="postgres",
        password="mysecretpassword"
    )

def drop_indexes(_conn):
    # Drop indexes
    cur = _conn.cursor()
    cur.execute('DROP INDEX IF EXISTS coord_index')
    cur.execute('DROP INDEX IF EXISTS user_index')
    cur.execute('DROP INDEX IF EXISTS time_index')

    # drop primary key constraint
    cur.execute('ALTER TABLE points DROP CONSTRAINT IF EXISTS points_pkey')

    _conn.commit()

def create_indexes(_conn):
    start = time.time()
    cur = _conn.cursor()
    print("Creating index coord_index")
    cur.execute("""
        CREATE INDEX coord_index IF NOT EXISTS
        ON points (coord_x, coord_y);
    """)
    print("Creating index user_index")
    cur.execute("""
        CREATE INDEX user_index IF NOT EXISTS
        ON points (user_name);
    """)
    print("Creating index time_index")
    cur.execute("""
        CREATE INDEX time_index IF NOT EXISTS
        ON points (time);
    """)

    # add primary key constraint
    print("Adding primary key constraint")
    cur.execute("""
        ALTER TABLE points
        ADD PRIMARY KEY (time, coord_x, coord_y, user_name) IF NOT EXISTS;
    """)

    _conn.commit()

    print("Indexes created in {} seconds".format(time.time() - start))
