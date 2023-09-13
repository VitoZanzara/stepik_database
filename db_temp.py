import sqlalchemy as db
from pathlib import Path

DBASE_PATH = 'db_files/{}'


def create_engine(db_name: str):
    db_path = DBASE_PATH.format(db_name)
    engine = db.create_engine(f'sqlite:///{db_path}')
    return engine


def make_text_query(query: str, db_name='temp_db.sqlite3'):
    engine = create_engine(db_name)
    # print(f'{db.text(query).key = }')
    with engine.connect() as conn:
        res = conn.execute(db.text(query))
        conn.commit()       # запись изменений в базу
    # if res.fetch
    # return res
    try:
        return res.fetchall()
    except:
        return res


def get_bd_info(db_name: str):
    engine = create_engine(db_name)
    meta = db.MetaData()
    meta.reflect(engine)
    return {'table_names': [t.name for t in meta.tables.values()],
            'full_description': list(meta.tables.values())}


def create_table_employee(db_name='temp_db.sqlite3'):
    table_name = 'employee'
    query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY, 
    name VARCHAR(128) NOT NULL,
    lastname VARCHAR(128) NOT NULL, 
    salary INTEGER CHECK(salary >= 0) DEFAULT 0)"""
    res = make_text_query(query, db_name)
    return res


def fill_employee(n_items=8, db_name='temp_db.sqlite3'):
    table_name = 'employee'
    from random import randint, sample
    from string import ascii_letters as abc
    items = []
    for i in range(1, n_items + 1):
        s = f'({i}, "{"".join(sample(abc, 8))}", "{"".join(sample(abc, 10))}", {randint(100, 500)})'
        items.append(s)
    query = f"""INSERT INTO {table_name} (id, name, lastname, salary) VALUES {f", ".join(items)};"""
    print(query)
    return make_text_query(query, db_name)


def drop_table(db_name, table_name):
    query = f"""DROP TABLE IF EXISTS {table_name}"""
    res = make_text_query(query, db_name)
    return res


if __name__ == '__main__':
    # print(create_table_employee())
    DB_NAME = 'temp_db.sqlite3'
    query_show_tables = "SELECT name FROM sqlite_master WHERE type='table';"
    # print(drop_table(DB_NAME, 'employee'))
    # print(get_bd_info(DB_NAME).get('table_names'))
    # print(make_text_query(DB_NAME, query_show_tables))
    fill_res = fill_employee()
    # print(fill_res.fetchall())
    # query_add_item = "INSERT INTO employee (name, lastname, salary) VALUES ('efg', 'aaaa', 400);"
    # make_text_query(query_add_item)
    print(make_text_query('SELECT * FROM employee').fetchall())

    # stmt = db.insert('employee').values(name="spongebob", fullname="Spongebob Squarepants")
