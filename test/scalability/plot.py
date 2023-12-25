import matplotlib.pyplot as plt
import numpy as np
import pickle
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.style.use('_mpl-gallery')

# make data
x = np.arange(1, 1001, 1)

with open('ay.pickle', 'rb') as file:
    # 使用pickle.load()从文件中加载对象
    ay= pickle.load(file)
with open('by.pickle', 'rb') as file:
    # 使用pickle.load()从文件中加载对象
    by= pickle.load(file)
y = np.vstack([ay, by])

# plot

colors = ['#FF7F0E','#1F77B4']
labels = ['Topology optimizer', 'Function scheduler']
plt.figure(figsize=(4, 2.5))
# fig, ax = plt.subplots(figsize=(4, 2.5))

plt.stackplot(x, y, labels=labels, colors=['#9467bd','#ffdb58'])
plt.ylabel(u'Time(s)', fontsize=16)
plt.xlabel(u'Function number', fontsize=16)
plt.tick_params(axis='y', labelsize=14)
plt.tick_params(axis='x', labelsize=14)
plt.tight_layout()
plt.legend(loc='upper left',fontsize=12)
plt.savefig('scalability.png')
plt.show()
