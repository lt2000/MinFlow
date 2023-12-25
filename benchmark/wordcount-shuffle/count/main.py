import hashlib
import re
import pickle
import time
import mmh3


def main(store):
    # input
    start = time.time()
    input_file = store.fetch_global_input()
    store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end-start}
    
    start = time.time()
    split_ratio = store.fetch_io_keys('split_ratio')
    output_keys = store.fetch_io_keys('output')

    # input_data = pickle.loads(input_files)
    # count
    output_buckets = {}
    hash_start = 0
    hash_end = split_ratio - 1
    
    for idx in range(hash_start, hash_end + 1):
        output_buckets[idx] = {}

    lines = input_file.splitlines()
    worddict = {}
    
    for line in lines:
        wordlist = line.decode('utf-8').strip().split()
        for word in wordlist:
            if word not in worddict:
                w = mmh3.hash(word)
                bucket = w % split_ratio
                worddict[word] = bucket
                output_buckets[bucket][word] = 1
            else:
                bucket = worddict[word]
                output_buckets[bucket][word] += 1
    del input_file
    
    # split
    output_num = len(output_keys)
    step = (hash_end - hash_start + 1) // output_num
    split_output = {}
    for i in range(hash_start, hash_end + 1, step):
        file_name = output_keys[(i-hash_start)//step]
        split_output[file_name] = {'content':{}, 'hash_info':{}}
        for j in range(i, i + step):
            split_output[file_name]['content'][str(j)] = output_buckets[j]
        split_output[file_name]['hash_info'] = {'hash_start': i,
                                            'hash_end': i + step - 1, 'split_ratio': split_ratio}
        if store.to == 'DB+MEM':
            fname_split = file_name.split('_')
            cur_func = int(fname_split[0].split('-')[-1])
            next_func = int(fname_split[1].split('-')[-1])
            if (cur_func // store.bundling_size) % store.node == (next_func // store.bundling_size) % store.node:
                store.db_mem[file_name] = 1
            else:
                store.db_mem[file_name] = 0 
                
    end = time.time()
    compute_time = {'start': start, 'end': end, 'latency': end-start}
    
    
    # output
    start = time.time()
    store.put(split_output, {})
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))
