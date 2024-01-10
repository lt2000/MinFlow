import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sys


worlflow_name = 'e040ec88-1574-49e0-9b87-be43cfee8bdd'
node_num = 10
bin_num = 100

def cpu():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        # print(i)
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['cpu']
        
    # Create a heat map for cpu utilization
    plt.figure(figsize=(5, 3))
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=14)           
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('cpu.png')
    plt.show() 

def mem():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['mem']
        
    # Create a heat map for memory utilization
    plt.figure(figsize=(5, 3))
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=14)           
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('mem.png')
    plt.show()    
        

def sent():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['sent']

    # Create a heat map for sent throughput
    plt.figure(figsize=(5, 3))
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=14)           
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('sent.png')
    plt.show()    


def recv():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['recv']
    
    
    # Create a heat map for receive throughput
    plt.figure(figsize=(5, 3))
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=14)           
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('recv.png')
    plt.show()  


if __name__ == '__main__':
    worlflow_name = sys.argv[1]
    node_num = int(sys.argv[2])
    cpu()
    print('Draw CPU')
    mem()
    print('Draw Mem')
    sent()
    print('Draw Sent')
    recv()
    print('Draw Recv')