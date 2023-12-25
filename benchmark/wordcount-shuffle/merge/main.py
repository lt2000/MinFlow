import time
import pickle

def main(store):
    # input
    input_keys = store.fetch_io_keys('input')
    
    start = time.time()
    input_files = store.fetch(input_keys)
    # store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end-start}
    
    start = time.time()
    hash_idx = input_files[input_keys[0]]['hash_info']
    output_keys = store.fetch_io_keys('output')

    # merge
    output_buckets = {}
    hash_start = hash_idx['hash_start']
    hash_end = hash_idx['hash_end']
    split_ratio = hash_idx['split_ratio']
    for idx in range(hash_start, hash_end + 1):
        output_buckets[idx] = {}
    
    for bucket_idx in range(hash_start, hash_end + 1):
        for file in input_files:
            if input_files[file] != 'NoSuchKey':
                bucket = input_files[file]['content'][str(bucket_idx)]
                for word in bucket:
                    bucket_dict = output_buckets[bucket_idx]
                    try:
                        bucket_dict[word] += bucket[word]
                    except:
                        bucket_dict[word] = bucket[word]
   
    del input_files
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
    end = time.time()
    compute_time = {'start': start, 'end': end, 'latency': end-start}
    
    
    # output
    start = time.time()
    store.put(split_output, {})
    store.clear()
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))