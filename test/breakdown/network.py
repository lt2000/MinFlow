import sys
import copy
sys.path.append('../../config')
import config
INFINITY = 1000000



class min_generator:

    def __init__(self, m, shuffle_mode):
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
        func_bw = config.FUNC_BANDWIDTH_LIMIT / 8000
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

        # print(transmit_time_dict)
        sorted_items = sorted(transmit_time_dict.items(), key=lambda x: x[1])
        sorted_dict = dict(sorted_items)
        opt_level = list(sorted_dict.keys())[0]
        if opt_level % 2 == 0:
            opt_level += 1
        opt_net = factorization_copy[opt_level]
        return opt_net
        
    # m: the number of mappers; r: the number of reducers, Suppose m = r
    def min_optimized(self):
        # Using dynamic programming to find factors and minimal factorization
        maxLevel = self.getPrimeNum(self.m)
        factors = self.getFactor(self.m)
        factors.append(self.m)
        factorization = self.dynamic_programming(maxLevel, factors)
        
        if self.modeler:
            opt_net = self.config_modeler(factorization)
            # opt_net = factorization[3]
        else: 
            if config.LAMBADA_OPT:
                opt_net = factorization[2]
            else:
                min_v = INFINITY
                for k, v in factorization.items():
                    if min_v > sum(v):
                        min_idx = k
                        min_v = sum(v)
                opt_net = factorization[min_idx]
                    
        print(opt_net)
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

if __name__ == "__main__":
    n = int(sys.argv[1])
    net = min_generator(n)
    print(net.group_ratio)
    print(net.foreach_size)
    print(net.group_num)
 

