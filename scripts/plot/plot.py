import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.font_manager import FontProperties



def runtime():
    df = pd.read_csv('performance.csv')
    fig, (ax1,ax2) = plt.subplots(1,2,figsize=(6, 2.5), dpi=120)
    width = 0.75
    markers = ['o', 'v', '^', '<', '>', 's', 'p', '*', 'X', 'd']
    color_name = 'Set3'
    bar_colors = ['#A63737', '#F2CF63', '#3E6E6B', '#7F007F', 'white']
  

    x = 0.5 + np.arange(5)
    for k, idx in enumerate(x):
        ax1.bar(idx, df['runtime'][k], width, color=bar_colors[k], ec='black', label=df['method'][k])
    ax1.set_ylabel(u'Execution time(s)', fontproperties='SimHei', fontsize=12)
    ax1.axes.xaxis.set_visible(False)
    ax1.set_ylim((0, 1.15))
    ax1.set_title('(a)', y=-0.2)

    for k, idx in enumerate(x):
        ax2.bar(idx, df['cost'][k], width, color=bar_colors[k], ec='black', label=df['method'][k])
    ax2.set_ylabel(u'Storage cost($)', fontproperties='SimHei', fontsize=12)
    ax2.axes.xaxis.set_visible(False)
    ax2.set_ylim((0, 1.15))
    ax2.set_title('(b)', y=-0.2)
    # fig.legend(loc='best')
    handles, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(handles, labels, loc = 'upper center', ncols=5)
    plt.tight_layout()
    plt.show()

def cost():
    df = pd.read_csv('cost.csv')
    fig, ax = plt.subplots(figsize=(5, 3), dpi=120)
    width = 0.5
    markers = ['o', 'v', '^', '<', '>', 's', 'p', '*', 'X', 'd']
    bar_colors = ['#130074', '#CB181B', 'white']
    ax.bar(df['method'], df['norm'], width, color=bar_colors, ec='black')
    ax.set_ylabel(u'Normalized cost', fontproperties='SimHei', fontsize=12)
    # ax.set_title('Wordcount Cost Simulation')
    plt.tight_layout()
    plt.show()
    
    pass
 
if __name__ == '__main__':
    runtime()
    #cost()
   
    pass
