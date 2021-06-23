import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df = pd.read_csv('query_times_finals.csv')

df['log_time'] = np.log2(df['time'])
ndf = df.drop(['records_returned', 'time'], axis=1)

bars = ndf.plot.bar(x='name')
fig = bars.get_figure()
params = {'mathtext.default': 'regular' }
plt.rcParams.update(params)
fig.suptitle("TPCH Query Run Times ($log_{2}$)", fontsize=16)
plt.ylabel('Run Time ($log_{2}$)')
plt.xlabel('Query Name')
plt.gcf().subplots_adjust(bottom=0.33)
fig.savefig("log_plot.png")