import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# df = pd.read_csv('query_times_finals.csv')
#
# df['log_time'] = np.log2(df['time'])
df = pd.read_csv('optimized_query_17.csv')
ndf = df.drop(['records_returned'], axis=1)


def build_graph(df, save_location, x, xlabel, ylabel, title):

    bars = df.plot.bar(x=x)
    fig = bars.get_figure()
    params = {'mathtext.default': 'regular' }
    plt.rcParams.update(params)
    fig.suptitle(title, fontsize=16)
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.gcf().subplots_adjust(bottom=0.33)
    fig.savefig(save_location)

# build_graph(ndf, "log_plot.png", 'name', 'Query Name', 'Run Time ($log_{2}$)', "TPCH Query Run Times ($log_{2}$)")
build_graph(ndf, "query17_plot.png", 'name', 'Query Name', 'Run Time', "TPCH Query 17 Run Times")