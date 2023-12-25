import struct
import numpy as np
import pickle
import time
import byteconcat

def main(store):
    # input
    start = time.time()
    input_file = store.fetch_global_input()
    store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end - start}

    start = time.time()
    split_ratio = store.fetch_io_keys('split_ratio')
    output_keys = store.fetch_io_keys('output')
    input_data = pickle.loads(input_file)

    
    del input_file
    
    # partition
    output_buckets = {}
    hash_start = 0
    hash_end = split_ratio - 1
    
    for idx in range(hash_start, hash_end + 1):
        output_buckets[idx] = []

    min_value = struct.unpack(">I", b"\x00\x00\x00\x00")[0]
    max_value = struct.unpack(">I", b"\xff\xff\xff\xff")[0]
    numPartitions = split_ratio
    rangePerPart = (max_value - min_value) // numPartitions

    
    boundaries = [] # (numPartitions-1) boundaries
    for idx in range(1, numPartitions):
        b = struct.pack('>I', rangePerPart * idx) # 4 bytes unsigned integers
        boundaries.append(b)

    part_recordType = np.dtype([('key', 'S4'), ('value', 'S96')])
    records = np.frombuffer(input_data, dtype=part_recordType)
    

    if numPartitions == 1:
        ps = ([0] * len(records))
    else:
        ps = np.searchsorted(boundaries, records['key'])
    del records
    
    record_len = 100
    for idx in range(0, len(input_data), record_len):
        output_buckets[ps[idx//record_len]].append(input_data[idx:idx + record_len])
    del input_data     
    

    # split
    output_num = len(output_keys)
    step = (hash_end - hash_start + 1) // output_num
    split_output = {}
    split_output_type = {}
    length = 0
    for i in range(hash_start, hash_end + 1, step):
        file_name = output_keys[(i-hash_start)//step]
        serialized_data = {'content':{}, 'hash_info':{}}
        split_output_type[file_name] = 'application/bytes'
        for j in range(i, i + step):
            serialized_data['content'][str(j)] = [byteconcat.concat_bytes(output_buckets[j])]
            length += len(serialized_data['content'][str(j)])
            del output_buckets[j]

        serialized_data['hash_info'] = {'hash_start': i,
                                            'hash_end': i + step - 1, 'split_ratio': split_ratio}
        split_output[file_name] = pickle.dumps(serialized_data)
        del serialized_data
        
        if store.to == 'DB+MEM':
            fname_split = file_name.split('_')
            cur_func = int(fname_split[0].split('-')[-1])
            next_func = int(fname_split[1].split('-')[-1])
            if (cur_func // store.bundling_size) % store.node == (next_func // store.bundling_size) % store.node:
                store.db_mem[file_name] = 1
            else:
                store.db_mem[file_name] = 0
                
    end = time.time()
    compute_time = {'start': start, 'end': end, 'latency': end - start}  

    # output
    start = time.time()
    store.put(split_output, split_output_type)
    store.clear()
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))
