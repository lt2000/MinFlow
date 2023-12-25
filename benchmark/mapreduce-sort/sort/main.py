import numpy as np
import pickle
import time
import byteconcat


def main(store):
    # input
    input_keys = store.fetch_io_keys('input')
    output_keys = store.fetch_io_keys('output')
    next_func = output_keys[0].split('_')[-1]
    
    start = time.time()
    input_files = store.fetch(input_keys)
    store.clear()
    end = time.time()
    input_time = {'start': start, 'end': end, 'latency': end-start}  
    
    start = time.time()
    hash_idx = pickle.loads(input_files[input_keys[0]])['hash_info']

    deserialized_data = {}
    for file in input_keys:
        deserialized_data[file] = pickle.loads(input_files[file])
        del input_files[file]
        
    
    # sort
    output_buckets = {}
    hash_start = hash_idx['hash_start']
    hash_end = hash_idx['hash_end']
    split_ratio = hash_idx['split_ratio']

    if 'global-output' in next_func:
        sort_recordType = np.dtype(
            [('key', 'S10'), ('value', 'S90')])  # 10 bytes for sorting
        for bucket_idx in range(hash_start, hash_end + 1):
            input_bucket = []
            for file in input_keys:
                concat_data = byteconcat.concat_bytes(deserialized_data[file]['content'][str(bucket_idx)])
                del deserialized_data[file]['content'][str(bucket_idx)]
                input_bucket.append(np.frombuffer(concat_data, dtype=sort_recordType))
                del concat_data

            data = np.concatenate(input_bucket)
            del input_bucket
            output_buckets[bucket_idx] = np.sort(data, order='key')
            del data

        # split
        output_num = len(output_keys)
        step = (hash_end - hash_start + 1) // output_num
        split_output = {}
        split_output_type = {}
        for i in range(hash_start, hash_end + 1, step):
            file_name = output_keys[(i-hash_start)//step]
            serialized_data = {'content':{}, 'hash_info':{}}
            split_output_type[file_name] = 'application/bytes'
            for j in range(i, i + step):
                serialized_data['content'][str(j)] = np.asarray(output_buckets[j]).tobytes()
                del output_buckets[j]
            serialized_data['hash_info'] = {'hash_start': i,
                                                'hash_end': i + step - 1, 'split_ratio': split_ratio}
            split_output[file_name] = pickle.dumps(serialized_data)
            del serialized_data
        end = time.time()
        compute_time = {'start': start, 'end': end, 'latency': end-start}
    else:
        for bucket_idx in range(hash_start, hash_end + 1):
            input_bucket = []
            for file in input_keys:
                input_bucket.extend(deserialized_data[file]['content'][str(bucket_idx)])
                del deserialized_data[file]['content'][str(bucket_idx)]
                
            output_buckets[bucket_idx] = input_bucket
            del input_bucket

        # split
        output_num = len(output_keys)
        step = (hash_end - hash_start + 1) // output_num
        split_output = {}
        split_output_type = {}
        for i in range(hash_start, hash_end + 1, step):
            file_name = output_keys[(i-hash_start)//step]
            serialized_data = {'content':{}, 'hash_info':{}}
            split_output_type[file_name] = 'application/bytes'
            for j in range(i, i + step):
                serialized_data['content'][str(j)] = output_buckets[j]
                del output_buckets[j]
            serialized_data['hash_info'] = {'hash_start': i,
                                                'hash_end': i + step - 1, 'split_ratio': split_ratio}
            split_output[file_name] = pickle.dumps(serialized_data)
            del serialized_data
        end = time.time()
        compute_time = {'start': start, 'end': end, 'latency': end-start}        
    
    # output
    start = time.time()
    store.put(split_output, split_output_type)
    store.clear()
    end = time.time()
    output_time = {'start': start, 'end': end, 'latency': end-start}
    log = {'input_time': input_time, 'compute_time': compute_time, 'output_time': output_time}
    key = output_keys[0].split('_')[0]
    store.log(key, pickle.dumps(log))
