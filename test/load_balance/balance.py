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
        
        # df_slices = np.array_split(df, bin_num)
        # for idx, df_slice in enumerate(df_slices):
        #     avg = df_slice.mean()
        #     usage[f'{i+1}'].append(avg.values[0])
    #     bins = pd.cut(df['mem'], bins=bin_num, labels=False)  # 使用 labels=False 获取分组的整数编码
    #     df[f'mem_bin'] = bins
        
    #     # df_list.append(pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes))
    #     bin_columns = [f'{column}' for column in df.columns if '_bin' in column]
        
    #     # 获取每份的平均值
    #     bin_means = df.groupby(bin_columns).mean()
    #     usage[f'{i+1}'] = list(bin_means['mem'])
    #     print(len(usage[f'{i+1}']))
    
    # df = pd.DataFrame(usage)
    plt.figure(figsize=(5, 3))
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    # 获取颜色条对象
    cbar = ax.collections[0].colorbar

    # 设置图例文字大小为12
    cbar.ax.tick_params(labelsize=14)           
    # plt.title('Node Load Over Time')
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('cpu.png')
    plt.show() 
       
        
    pass

def mem():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        # print(i)
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['mem']
        # df = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)[['mem']]
        # df_slices = np.array_split(df, bin_num)
        # for idx, df_slice in enumerate(df_slices):
        #     avg = df_slice.mean()
        #     usage[f'{i+1}'].append(avg.values[0])
    #     bins = pd.cut(df['mem'], bins=bin_num, labels=False)  # 使用 labels=False 获取分组的整数编码
    #     df[f'mem_bin'] = bins
        
    #     # df_list.append(pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes))
    #     bin_columns = [f'{column}' for column in df.columns if '_bin' in column]
        
    #     # 获取每份的平均值
    #     bin_means = df.groupby(bin_columns).mean()
    #     usage[f'{i+1}'] = list(bin_means['mem'])
    #     print(len(usage[f'{i+1}']))
    
    plt.figure(figsize=(5, 3))
    # 创建热图
    
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    # 获取颜色条对象
    cbar = ax.collections[0].colorbar

    # 设置图例文字大小为12
    cbar.ax.tick_params(labelsize=14)           
    # plt.title('Node Load Over Time')
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    # plt.xlabel('Node')
    plt.tight_layout()
    plt.savefig('mem.png')
    plt.show()    
        
    pass

def sent():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        # print(i)
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['sent']
    # for i in range(node_num):
    #     usage[f'{i+1}'] = []
    #     print(i)
    #     filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
    #     df = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)[['sent']]
    #     df_slices = np.array_split(df, bin_num)
    #     for idx, df_slice in enumerate(df_slices):
    #         avg = df_slice.mean()
    #         usage[f'{i+1}'].append(avg.values[0])
    #     bins = pd.cut(df['mem'], bins=bin_num, labels=False)  # 使用 labels=False 获取分组的整数编码
    #     df[f'mem_bin'] = bins
        
    #     # df_list.append(pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes))
    #     bin_columns = [f'{column}' for column in df.columns if '_bin' in column]
        
    #     # 获取每份的平均值
    #     bin_means = df.groupby(bin_columns).mean()
    #     usage[f'{i+1}'] = list(bin_means['mem'])
    #     print(len(usage[f'{i+1}']))
    
    # df = pd.DataFrame(usage)
    plt.figure(figsize=(5, 3))
    # 创建热图
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    # 获取颜色条对象
    cbar = ax.collections[0].colorbar

    # 设置图例文字大小为12
    cbar.ax.tick_params(labelsize=14)           
    # plt.title('Node Load Over Time')
    plt.tick_params(axis='x', labelsize=14)
    plt.ylabel('Time period(s)',fontsize=16)
    plt.xlabel('Node',fontsize=16)
    plt.tight_layout()
    plt.savefig('sent.png')
    plt.show()    
        
    pass

def recv():
    names = ['sent', 'recv', 'cpu', 'mem']
    dtypes = {'sent': 'float64', 'recv': 'float64', 'cpu': 'float64', 'mem': 'float64'}
    usage = {}
    df = pd.DataFrame()
    for i in range(node_num):
        usage[f'{i+1}'] = []
        # print(i)
        filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
        df[f'{i+1}'] = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)['recv']
    # for i in range(node_num):
    #     usage[f'{i+1}'] = []
    #     print(i)
    #     filename = f'./dataset/{worlflow_name}_node{i+1}.txt'
    #     df = pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes)[['sent']]
    #     df_slices = np.array_split(df, bin_num)
    #     for idx, df_slice in enumerate(df_slices):
    #         avg = df_slice.mean()
    #         usage[f'{i+1}'].append(avg.values[0])
    #     bins = pd.cut(df['mem'], bins=bin_num, labels=False)  # 使用 labels=False 获取分组的整数编码
    #     df[f'mem_bin'] = bins
        
    #     # df_list.append(pd.read_csv(filename, sep='|', header=None, names=names, dtype=dtypes))
    #     bin_columns = [f'{column}' for column in df.columns if '_bin' in column]
        
    #     # 获取每份的平均值
    #     bin_means = df.groupby(bin_columns).mean()
    #     usage[f'{i+1}'] = list(bin_means['mem'])
    #     print(len(usage[f'{i+1}']))
    
    # df = pd.DataFrame(usage)
    plt.figure(figsize=(5, 3))
    # 创建热图
    ax = sns.heatmap(df, cmap='OrRd', annot=False)
    ytick_positions = [0,100, 200, 300, 400, 500,600]
    ax.set_yticks(ytick_positions)
    ytick_labels = [0,5, 10, 15, 20, 25, 30]
    ax.set_yticklabels(ytick_labels, fontsize=14)
    # 获取颜色条对象
    cbar = ax.collections[0].colorbar

    # 设置图例文字大小为12
    cbar.ax.tick_params(labelsize=14)           
    # plt.title('Node Load Over Time')
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