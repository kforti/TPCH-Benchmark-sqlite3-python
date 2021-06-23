import pandas as pd
import numpy as np

df = pd.read_csv('query_times_finals.csv')

df['log_time'] = np.log2(df['time'])

bars = df['log_time'].plot.bar(x='name')
fig = bars.get_figure()
fig.savefig("log_plot.png")