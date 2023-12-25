import pandas as pd
import numpy as np

# 创建一个示例 DataFrame
data = {
    'A': np.arange(23),  # 随机生成的数据
    'B': np.arange(23),
    'C': np.arange(23)
}

df = pd.DataFrame(data)


# 将每列划分成10份，并计算每份的平均值
num_bins = 10  # 划分的份数

# 使用 cut() 函数划分每列
for column in df.columns:
    bins = pd.cut(df[column], bins=num_bins, labels=False)  # 使用 labels=False 获取分组的整数编码
    df[f'{column}_bin'] = bins

print(df)
# 使用 groupby() 和 mean() 计算每份的平均值
bin_columns = [f'{column}' for column in df.columns if '_bin' in column]

# 获取每份的平均值
bin_means = df.groupby(bin_columns).mean()

# 打印结果
print(bin_means)
print(list(bin_means['A']))
