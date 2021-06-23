import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt


df = pd.read_csv('query_times.csv')
fig = df.hist(bins=2)
fig[0][1].get_figure().show()

# values = df['time'].to_numpy()
# names = df['name'].to_numpy()
#
# binned_means = stats.binned_statistic(values, 'mean', bins=2)
# for i in binned_means:
#     print(i)
# plt.figure()
# plt.plot(binned_means, names)
# plt.show()