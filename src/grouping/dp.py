import sys
import copy
import time
sys.path.append('../../config')
import config
INFINITY = 1000000000

def getPrimeNum(n):
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

def getFactor(num):

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

def dynamic_programming(maxLevel, factors):
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
                for k in getFactor(j):
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
    
    
def modeler(factorization):
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
    print(opt_net)
      

if __name__ == "__main__":
    n = int(sys.argv[1])
    start = time.time()
    maxLevel = getPrimeNum(n)
    factors = getFactor(n)
    factors.append(n)
    factorization = dynamic_programming(maxLevel, factors)
    print(factorization)
    modeler(factorization)
    end = time.time()
    print(end-start)