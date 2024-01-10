import sys
import copy
import time
import matplotlib.pyplot as plt
import numpy as np
import  multiprocessing
import pickle
from functools import partial
sys.path.append('../../config')
import config
shuffle_mode = config.SHUFFLE_MODE
INFINITY = 1000000



class min_generator:

    def __init__(self, m):
        self.m = m
        self.group_ratio = []
        self.group_num = []
        self.foreach_size = []
        self.modeler = config.MODELER
        self.min_optimized()
        
        if shuffle_mode == 'min':
            self.shuffle_n = len(self.group_ratio)
        elif self.is_prime(m):
            self.shuffle_n = 1
            self.foreach_size = [m,m]
        else:
            self.shuffle_n = 1
            foreach_size = self.foreach_size[0]
            self.foreach_size = [foreach_size, foreach_size]
            
    def baseline_hash(self, idx, function_stage_idx):
        input = []
        output = []
        split_ratio = self.m
        group_size = [self.group_num[0]//i for i in self.group_num]
        function_stage_num = len(group_size)
        # output
        if function_stage_idx != function_stage_num - 1:
            for next_function_idx in range(split_ratio):
                if (idx // group_size[function_stage_idx+1]) == (next_function_idx // group_size[function_stage_idx+1]) and idx % group_size[function_stage_idx] == (next_function_idx // (group_size[function_stage_idx+1] // group_size[function_stage_idx])) % group_size[function_stage_idx]:
                    output.append(next_function_idx)
        # input
        if function_stage_idx != 0:
            for pre_function_idx in range(split_ratio):
                if (pre_function_idx // group_size[function_stage_idx]) == (idx // group_size[function_stage_idx]) and pre_function_idx % group_size[function_stage_idx-1] == (idx // (group_size[function_stage_idx] // group_size[function_stage_idx-1])) % group_size[function_stage_idx-1]:
                    input.append(pre_function_idx)

        return input, output

    def schedule_hash(self, idx, function_stage_idx, input, output):
        new_input = []
        new_output = []
        group_size = [self.group_num[0]//i for i in self.group_num]
        group_ratio = [group_size[i] // group_size[i-1] for i in range(1, len(group_size))]
        hash_input = []
        hash_output = []
        hash_ioput = []
        fucntion_stage_num = len(group_size)
        for i in range(fucntion_stage_num):
            if i % 2 == 0 and i != 0 and i !=fucntion_stage_num - 1:
                hash_ioput.append(i)
                hash_output.append(i-1)
                hash_input.append(i+1)

        # print(hash_ioput, hash_input, hash_output)
        if function_stage_idx in hash_ioput:
            origin_idx = (idx % group_ratio[function_stage_idx])*group_size[function_stage_idx] + (
                idx//group_ratio[function_stage_idx])%group_size[function_stage_idx] + (
                idx//group_size[function_stage_idx+1])*group_size[function_stage_idx+1]
            new_input, new_output = self.baseline_hash(origin_idx, function_stage_idx)

        elif function_stage_idx in hash_input and function_stage_idx in hash_output:
            for base_idx in input:
                 new_input.append((base_idx // group_size[function_stage_idx - 1])%group_ratio[function_stage_idx - 1] + (
                     base_idx % group_size[function_stage_idx - 1]) * group_ratio[function_stage_idx - 1] + (
                     base_idx//group_size[function_stage_idx])*group_size[function_stage_idx])
            for base_idx in output:
                new_output.append((base_idx // group_size[function_stage_idx+1])%group_ratio[function_stage_idx+1] + (
                     base_idx % group_size[function_stage_idx+1]) * group_ratio[function_stage_idx+1] + (
                     base_idx//group_size[function_stage_idx+2])*group_size[function_stage_idx+2])
            
        elif function_stage_idx in hash_input:
            new_output = output
            for base_idx in input:
                 new_input.append((base_idx // group_size[function_stage_idx - 1])%group_ratio[function_stage_idx - 1] + (
                     base_idx % group_size[function_stage_idx - 1]) * group_ratio[function_stage_idx - 1] + (
                     base_idx//group_size[function_stage_idx])*group_size[function_stage_idx])

        elif function_stage_idx in hash_output:
            new_input = input
            for base_idx in output:
                new_output.append((base_idx // group_size[function_stage_idx+1])%group_ratio[function_stage_idx+1] + (
                     base_idx % group_size[function_stage_idx+1]) * group_ratio[function_stage_idx+1] + (
                     base_idx//group_size[function_stage_idx+2])*group_size[function_stage_idx+2])
        else:
            new_input = input
            new_output = output

        return new_input, new_output
    
    def is_prime(self, n):  # Determine if n is prime
        if n == 1 or n == 2:
            return True
        for i in range(2, n//2 + 1):
            if n % i == 0:
                return False
        return True
            
    def getPrimeNum(self, n):
        cont = 0
        factor = 2
        while n!=factor:
            if n % factor == 0:
                cont +=1
                n = n // factor
            else:
                factor+=1
        cont +=1
        return cont

    def getFactor(self, num):

        factors = []  # List to store the factors of the number
        i = 2
        while i * i <= num:
            if num % i == 0:
                factors.append(i)
                if i != num // i:  # 避免重复添加，例如对于平方数。
                    factors.append(num // i)
            i += 1
        factors.sort()

        return factors  # 因为我们现在是在找到因子时就添加，所以最终返回前要排序。

    def dynamic_programming(self, maxLevel, factors):
        # initialize
        minSum = {}
        sol = {}
        for j in factors:
            minSum[j] = {}
            sol[j] = {}
            for i in range(1,maxLevel + 1):
                minSum[j][i] = INFINITY
                sol[j][i] = 0

        # dp        
        for i in range(1, maxLevel + 1):
            for j in factors:
                if i == 1:
                    minSum[j][i] = j
                    sol[j][i] = j
                else:
                    for k in self.getFactor(j):
                        if minSum[j][i] > k + minSum[j//k][i-1]:
                            minSum[j][i] = k + minSum[j//k][i-1]
                            sol[j][i] = k

        # reconstruction
        # for i in range(1,maxLevel + 1):
        #     for j in factors:
        #         print(j,i,minSum[j][i], sol[j][i])     

        factorization = {}
        n = factors[-1]
        for i in range(1,maxLevel + 1):
            factorization[i] = []
            last = 1
            for j in range(i):
                factorization[i].append(sol[n//last][i-j])   
                last *= sol[n//last][i-j]
        return factorization


    def config_modeler(self, factorization):
        write_limit = config.S3_WRITE_QPS_LIMIT 
        read_limit = config.S3_READ_QPS_LIMIT
        func_bw = config.FUNC_BANDWIDTH_LIMIT
        reids_bw = config.REDIS_BANDWIDTH_LIMIT
        redis_qps = config.REDIS_QPS_LIMIT
        node_num = config.NODE_NUM
        inter_input_ratio = config.INTER_INPUT_RATIO
        inter_data_size = config.INPUT_DATA_SIZE * inter_input_ratio[config.WORKFLOW_NAME]
        func_num = 0

        transmit_time_dict = {}
        factorization_copy = copy.deepcopy(factorization)
        for level in factorization:
            if level == 1:
                func_num = factorization[level][0]
                continue
            factors = factorization[level]
            transmit_time_sum = 0
            turn = 1
            while(len(factors)):
                if turn: 
                    req_num = factors.pop(-1) * func_num
                    t1 = inter_data_size / (func_num * reids_bw)
                    t2 = req_num / (node_num * redis_qps)
                    transmit_time_sum += 2*max(t1,t2)
                    turn = 0
                else:
                    req_num = factors.pop(0) * func_num
                    t1 = inter_data_size / (func_num * func_bw)
                    t2 = req_num / write_limit
                    transmit_time_sum += max(t1,t2)
                    t2 = req_num / read_limit
                    transmit_time_sum += max(t1,t2)
                    turn = 1

            transmit_time_dict[level] = transmit_time_sum


        sorted_items = sorted(transmit_time_dict.items(), key=lambda x: x[1])
        sorted_dict = dict(sorted_items)
        opt_level = list(sorted_dict.keys())[0]
        opt_net = factorization_copy[opt_level]
        return opt_net
        
    # m: the number of mappers; r: the number of reducers, Suppose m = r
    def min_optimized(self):
        if self.m == 1:
            self.group_num = [1,1]
            self.group_ratio = [1]
            self.foreach_size = [1,1]
            return
        # Using dynamic programming to find factors and minimal factorization
        maxLevel = self.getPrimeNum(self.m)
        factors = self.getFactor(self.m)
        factors.append(self.m)
        factorization = self.dynamic_programming(maxLevel, factors)
        opt_net = factorization[list(factorization.keys())[-1]]
                    
        # print(opt_net)
        self.group_ratio = []
        low = 0
        high = len(opt_net) - 1

        for i in range(low, high + 1):
            if low <= high:
                if i % 2 == 0:
                    self.group_ratio.append(opt_net[high])
                    high -= 1
                else:
                    self.group_ratio.append(opt_net[low])
                    low += 1
                    
        # calculate group_num list
        self.group_num.append(self.m)
        for idx, ratio in enumerate(self.group_ratio):
            last = self.group_num[idx]
            self.group_num.append(last//ratio)

        # calculate foreach_size list
        shuffle_n = len(self.group_ratio)
        for k,v in enumerate(self.group_ratio):
            if k % 2 == 0:
                self.foreach_size.extend([v, v])
            elif k + 1 == shuffle_n:
                # self.foreach_size.append(self.m) # The last level has only one function
                self.foreach_size.append(self.group_ratio[-2]) # The last level has the same number of functions as the previous level
        return 0
    
def split_list(input_list, num_partitions):
    avg = len(input_list) // num_partitions
    remainder = len(input_list) % num_partitions

    partitions = []
    start = 0

    for i in range(num_partitions):
        end = start + avg + (1 if i < remainder else 0)
        partitions.append(input_list[start:end])
        start = end

    return partitions

def worker_func(id_list, net):
    pass


def is_prime(number):
    if number <= 1:
        return False
    for i in range(2, int(number**0.5) + 1):
        if number % i == 0:
            return False
    return True

if __name__ == "__main__":
    #n = int(sys.argv[1])
    plt.style.use('_mpl-gallery')

    # make data
    x = np.arange(1, 1001, 1)
    ay = []
    by = []
    
    for a in range(1,5):
        print(f'alpha={a}')
        pdf = {}
        alpha_3 = {}
        if a>1:
            pdf[1]=0
        for n in range(1,1001,1):
            max_level = -1
            if n == 0:
                continue
            base = 0
            schedule = 0
            t1 = time.time()
            new_n = n
            if is_prime(n):
                # Pick a substitute for the prime number
                for k in range(max(1,n-a), n+a+1):
                    temp_net = min_generator(k)
                    if len(temp_net.group_ratio) > max_level:
                        max_level =  len(temp_net.group_ratio)
                        net = temp_net
                        new_n = k
                try:
                    pdf[max_level] += 1
                except:
                    pdf[max_level] = 1    
                alpha_3[n] = new_n
                # print(n,new_n,net.group_ratio)
            t2 = time.time()
            base += t2-t1 
         
        # print(pdf)
        # print(pdf)
        # print(sum(pdf.values()))
        with open('a{}.pickle'.format(a), 'wb') as file:
            pickle.dump(pdf, file)
        

        
    

 

