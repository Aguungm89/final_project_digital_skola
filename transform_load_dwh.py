#python3

import connection
import model
import pandas


from sqlalchemy import create_engine

if __name__ == "__main__":

    engine = create_engine('postgresql://postgres:sembarang89@localhost:5432/dwh_finpro')
    conf_postgresql = connection.param_config("datalake")
    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    list_tables = model.list_tables()
    for table in list_tables:
        query = table[1]
        cur.execute(query)
        data = cur.fetchall()

        df = pandas.DataFrame(data, columns=[col[0] for col in cur.description])

        df.to_sql(table[0], engine, if_exists='replace', index=False)
        print(table[0], "- done")






