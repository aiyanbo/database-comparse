import psycopg2
import logging
import psycopg2.extras


def load(config, db) -> dict:
    table_names = get_compare_tables(config.get("left"), db)
    left_tables = get_table_infos(config.get("left"), db, table_names)
    right_tables = get_table_infos(config.get("right"), db, table_names)
    return {"tables": table_names, "left": left_tables, "right": right_tables}


def get_table_infos(properties, db, table_names) -> dict:
    conn = get_connection(properties, db)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tables = {}
    try:
        for table_name in table_names:
            cursor.execute(f"SELECT count(id) FROM {table_name}")
            count = cursor.fetchone()
            cursor.execute(f"SELECT max(id) FROM {table_name}")
            max_id = cursor.fetchone()
            cursor.execute(f"SELECT last_value FROM {table_name}_id_seq")
            seq = cursor.fetchone()
            tables[table_name] = {"count": count, "max": max_id, "seq": seq}
        return tables
    except RuntimeError as e:
        logging.error(f"Get table info error {properties}")
        raise e
    finally:
        cursor.close()
        conn.close()


def get_compare_tables(properties, db) -> list:
    conn = get_connection(properties, db)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_type = 'BASE TABLE'"
        )
        rows = cursor.fetchall()
        return list(map(lambda row: row[0], rows))
    finally:
        cursor.close()
        conn.close()


def get_connection(properties, dbname):
    logging.info(f"Connect {properties}, db: {dbname}")
    return psycopg2.connect(dbname=dbname,
                            host=properties.get('host'), port=properties.get('port'),
                            user=properties.get('user'), password=properties.get('password'))
