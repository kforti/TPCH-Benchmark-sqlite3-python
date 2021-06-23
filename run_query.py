import csv
import sqlite3

import pandas as pd

from queries import *

con = sqlite3.connect('data/tpch.db')
cur = con.cursor()


def save_records(name, records):
    with open(f'data/query_results/{name}.csv', 'w') as f:
        csv_writer = csv.writer(f)
        for row in records:
            csv_writer.writerow(row)



data = []
futures = []
for name, query in queries.items():
    try:
        result, completion_time = query.run(cur)
    except Exception as e:
        print('Error with query', name, [i for i in result.fetchall()])
        print(e.with_traceback())
        continue
    records = [i for i in result.fetchall()]
    data.append({'name': name, 'time': completion_time, 'records_returned': len(records)})

    save_records(name, records)


df = pd.DataFrame(data)
df.to_csv('query_times_finals.csv')

# bars = df.plot.bar(x='name')
# fig = bars.get_figure()
# fig.savefig("plot.png")