import os

import pandas
from sqlalchemy import create_engine

if __name__ == "__main__":
    path = os.getcwd() + "\\" + "Dataset" + "\\"

    for dic in  [("distribution_centers.csv","distribution_centers","distribution_centers"),
                ("employees.csv","employee","employee"),
                ("users.csv","user","users"),
                ("inventory_items.csv","inventory_items","inventory_items"),
                ("order_items.csv","order","order_item"),
                ("orders.csv","order","order"),
                ("products.csv","product","product"),
                ("events.csv","events","events")]:

        df = pandas.read_csv(path + dic[0])

        engine = create_engine('postgresql://postgres:sembarang89@localhost:5432/datalake_finpro')
        df.to_sql(dic[2], engine, if_exists='replace', index=False)
        print(dic[2], "- done")