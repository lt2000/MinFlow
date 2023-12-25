import matplotlib.pyplot as plt
import networkx as nx
import random


class min_generator:

    def __init__(self, m):
        self.m = m
        self.group_ratio = []
        self.group_num = []
        self.foreach_size = []
        self.min_optimized()
        self.shuffle_n = len(self.group_ratio)

    def is_prime(self, n):  # Determine if n is prime
        if n == 1 or n == 2:
            return True
        for i in range(2, n//2 + 1):
            if n % i == 0:
                return False
        return True

    def min_sum_factorization(self, m):
        if m == 1 or m == 2:
            return m
        # If m(>2) has a factor, it must be less than or equal to m over 2
        for i in range(2, m//2 + 1):
            if m % i == 0:  # i is a factor of m
                # If the minimum sum is equal to itself then i is prime
                if self.min_sum_factorization(i) == i:
                    self.group_ratio.append(i)
                    factor = m//i
                    if self.is_prime(factor):
                        self.group_ratio.append(factor)
                    # Dynamic programming
                    return i + self.min_sum_factorization(factor)
        return m  # m is prime and returns itself

    # m: the number of mappers; r: the number of reducers, Suppose m = r
    def min_optimized(self):
        # Using dynamic programming to find factors and minimal factorization
        self.min_sum_factorization(self.m)

        # Combine two 2s into a 4 o reduce the number of stages, because 2x2=2+2
        local_group_ratio = []
        for i in range(len(self.group_ratio)-1, -1, -1):
            if self.group_ratio[i] != 2:
                local_group_ratio.append(self.group_ratio[i])
            else:
                for j in range((i+1)//2):
                    local_group_ratio.append(4)
                if (i+1) % 2 != 0:
                    local_group_ratio.append(2)
                break

        local_group_ratio.sort(reverse=True)
        self.group_ratio = []
        low = 0
        high = len(local_group_ratio) - 1

        for i in range(low, high + 1):
            if low <= high:
                if i % 2 == 0:
                    self.group_ratio.append(local_group_ratio[low])
                    low += 1
                else:
                    self.group_ratio.append(local_group_ratio[high])
                    high -= 1

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
                self.foreach_size.append(self.m)
        return 0

    def baseline_hash(self, idx, split_ratio, group_size, function_stage_idx):
        input = []
        output = []
        function_stage_num = len(group_size)
        # output
        if function_stage_idx != function_stage_num - 1:
            for next_function_idx in range(split_ratio):
                if (idx // group_size[function_stage_idx+1]) == ( next_function_idx // group_size[function_stage_idx+1]) and idx % group_size[function_stage_idx] == (next_function_idx // (group_size[function_stage_idx+1] // group_size[function_stage_idx])) % group_size[function_stage_idx]:
                    output.append(next_function_idx)
        # input
        if function_stage_idx != 0:
            for pre_function_idx in range(split_ratio):
                if (pre_function_idx // group_size[function_stage_idx]) == (idx // group_size[function_stage_idx]) and pre_function_idx % group_size[function_stage_idx-1] == (idx // (group_size[function_stage_idx] // group_size[function_stage_idx-1])) % group_size[function_stage_idx-1]:
                    input.append(pre_function_idx)

        return input, output
    
    def schedule_hash(self, idx, split_ratio, group_size, function_stage_idx, input, output):
       new_input = []
       new_output = []
       hash_input = []
       hash_output = []
       hash_ioput = []
       fucntion_stage_num = len(group_size)
       for i in range(fucntion_stage_num):
           if i % 2 == 0 and i != 0 and i !=fucntion_stage_num - 1:
               hash_ioput.append(i)
               hash_output.append(i-1)
               hash_input.append(i+1)
        
    #    print(hash_ioput, hash_input, hash_output)
       if function_stage_idx in hash_ioput:
           origin_idx = (idx % net.group_ratio[function_stage_idx])*group_size[function_stage_idx] + (
               idx//net.group_ratio[function_stage_idx])%group_size[function_stage_idx] + (
               idx//group_size[function_stage_idx+1])*group_size[function_stage_idx+1]
           print(idx, origin_idx)
           new_input, new_output = self.baseline_hash(origin_idx, split_ratio, group_size, function_stage_idx)

       elif function_stage_idx in hash_input:
           new_output = output
           for base_idx in input:
                new_input.append((base_idx // group_size[function_stage_idx - 1])%net.group_ratio[function_stage_idx - 1] + (
                    base_idx % group_size[function_stage_idx - 1]) * net.group_ratio[function_stage_idx - 1] + (
                    base_idx//group_size[function_stage_idx])*group_size[function_stage_idx])
                
       elif function_stage_idx in hash_output:
           new_input = input
           for base_idx in output:
               new_output.append((base_idx // group_size[function_stage_idx+1])%net.group_ratio[function_stage_idx+1] + (
                    base_idx % group_size[function_stage_idx+1]) * net.group_ratio[function_stage_idx+1] + (
                    base_idx//group_size[function_stage_idx+2])*group_size[function_stage_idx+2])
       else:
           new_input = input
           new_output = output
           
       return new_input, new_output

    # def schedule_hash(self, layer, idx, down):
    #     m = self.m
    #     if layer == 0:
    #         return idx
    #     if not ((layer % 2) ^ down):
    #         return idx // (m // self.group_num[layer+down]) + (idx % (m // self.group_num[layer+down])) * (self.group_num[layer+down])
    #     return idx


    def min_plot(self):  # plot multistage interconnection network
        left, right, bottom, top = .1, .9, .1, .9
        group_size = [self.group_num[0]//i for i in self.group_num]
        print(group_size)
        layer_num = len(self.group_num)
        layer_sizes = [self.group_num[0] for i in range(layer_num)]

        G = nx.Graph()
        v_spacing = (top - bottom)/float(max(layer_sizes))
        h_spacing = (right - left)/float(len(layer_sizes) - 1)
        node_count = 0
        for i, v in enumerate(layer_sizes):
            layer_top = v_spacing*(v-1)/2. + (top + bottom)/2.
            for j in range(v):
                G.add_node(node_count, pos=(
                    left + i*h_spacing, layer_top - j*v_spacing))
                node_count += 1

        # for x, (left_nodes, right_nodes) in enumerate(zip(layer_sizes[:-1], layer_sizes[1:])):
        #     for i in range(left_nodes):
        for x in range(net.shuffle_n):
            for i in range(net.m):
                input, output = net.baseline_hash(i, net.m, group_size, x)
                new_input, new_output = net.schedule_hash(i, net.m, group_size, x, input, output)
                for j in new_output:
                    G.add_edge(i+sum(layer_sizes[:x]), j+sum(layer_sizes[:x+1]))
                
                # for j in range(right_nodes):
                    # baseline network
                    # if (i // group_size[x+1]) == (j // group_size[x+1]) and i % group_size[x] == (j // (group_size[x+1] // group_size[x])) % group_size[x]:
                        # schedule network
                        # G.add_edge(self.schedule_hash(
                        #     x, i, 0)+sum(layer_sizes[:x]), self.schedule_hash(x, j, 1)+sum(layer_sizes[:x+1]))
                        # G.add_edge(i+sum(layer_sizes[:x]), j+sum(layer_sizes[:x+1]))

                    
                pass

        pos = nx.get_node_attributes(G, 'pos')
        # 把每个节点中的位置pos信息导出来
        nx.draw(G, pos,
                node_color='b',
                with_labels=False,
                node_size=50,
                # edge_color=[random.random() for i in range(len(G.edges))],
                width=1,
                # cmap=plt.cm.Dark2,  # matplotlib的调色板，可以搜搜，很多颜色呢
                edge_cmap=plt.cm.Blues
                )
        # plt.show()
        plt.savefig('./test.jpg')


if __name__ == "__main__":
    net = min_generator(27)
    # net.min_optimized(net.m)
    # print(net.group_num)
    # print(net.group_ratio)
    # print(net.foreach_size)
    net.min_plot()
    # group_size = [net.group_num[0]//i for i in net.group_num]
    # group_ratio = [group_size[i] // group_size[i-1] for i in range(1, len(group_size))]
    # print(net.group_ratio)
    # print(group_ratio)
    # idx = 1
    # function_stage_idx = 2
    # input, output = net.baseline_hash(idx, net.m, group_size, function_stage_idx)
    # new_input, new_output = net.schedule_hash(idx, net.m, group_size, function_stage_idx, input, output)
    # print(new_input)
    # print(new_output)

