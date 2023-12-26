import matplotlib.pyplot as plt
import numpy as np
import pickle

plt.rcParams['font.family'] = 'DejaVu Sans'
plt.style.use('_mpl-gallery')

with open('a1.pickle', 'rb') as file:
    a1= pickle.load(file)
with open('a2.pickle', 'rb') as file:
    a2= pickle.load(file)
with open('a3.pickle', 'rb') as file:
    a3= pickle.load(file)
with open('a4.pickle', 'rb') as file:
    a4= pickle.load(file)


fig, ax1 = plt.subplots(1,1,figsize=(4, 2.5), dpi=120)
x = []
y = []
cdf = 0
for k, v in a1.items():
    x.append(k)
    cdf += v
    y.append(cdf/168)
ax1.plot(x, y, label=r'$\alpha=1$', color='#D62026', marker='^',markerfacecolor='none')

x = []
y = []  
cdf = 0
for k, v in a2.items():
    x.append(k)
    cdf += v
    y.append(cdf/168)
ax1.plot(x, y, label=r'$\alpha=2$', color='#2162AF', marker='o',markerfacecolor='none')    

x = []
y = []   
cdf = 0
for k, v in a3.items():
    x.append(k)
    cdf += v
    y.append(cdf/168)
ax1.plot(x, y, label=r'$\alpha=3$', color='#F7951D', marker='d',markerfacecolor='none') 

x = []
y = []   
cdf = 0
for k, v in a4.items():
    x.append(k)
    cdf += v
    y.append(cdf/168)
ax1.plot(x, y, label=r'$\alpha=4$', color='#9467bd', marker='x',markerfacecolor='none')   

# 绘制累积分布折线图
ax1.set_xlabel('The number of network levels',fontsize=16)
ax1.set_ylabel('CDF', fontsize=16)
ax1.tick_params(axis='y', labelsize=12)
ax1.set_xticks(np.arange(1, 10, 1))
ax1.tick_params(axis='x', labelsize=12)
ax1.legend(fontsize=12)
plt.grid(True)
plt.tight_layout()
plt.savefig('alpha.png')
plt.show()
