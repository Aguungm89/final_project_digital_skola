#!python3

from importlib.metadata import distribution
import os
import connection

import pandas

from hdfs import InsecureClient
from datetime import datetime

if __name__ == "__main__":
    # print("test")

    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    path = os.getcwd() + "\\" + "dataset" + "\\"

    for dic in [("distribution_centers.csv","distribution_centers","distribution_centers"),
                ("employees.csv","employee","employee"),
                ("users.csv","user","user"),
                ("inventory_items.csv","inventory_items","inventory_items"),
                ("order_items.csv","order","order_item"),
                ("orders.csv","order","order"),
                ("products.csv","product","product"),
                ("events.csv","events","events")
                ]:

        df = pandas.read_csv(path + dic[0])

        time = datetime.now().strftime("%Y%m%d")
        with client.write(f'/Final Project/{dic[1]}/{dic[2]}_{time}.csv', encoding='utf-8') as writer:
                df.to_csv(writer, index=False)
        print(dic[1], "- done")





