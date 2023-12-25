import yaml

def test():
    ip_list = []
    node_info_list = yaml.load(open('node_info.yaml'), Loader=yaml.FullLoader)
    for node_info in node_info_list['nodes']:
        ip_list.append(node_info['worker_address'].split(':')[0])
    print(ip_list)
if __name__ == '__main__':
    test()
    