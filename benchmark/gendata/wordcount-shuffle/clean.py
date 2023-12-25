import re

# 打开输入文件和输出文件
with open('file9', 'r') as input_file, open('cleaned_data', 'w') as output_file:
    # 逐行处理输入文件
    for line in input_file:
        # 使用正则表达式替换非单词字符为空格，并保留换行符
        cleaned_line = re.sub(r'\W+', ' ', line.rstrip()) + '\n'
        output_file.write(cleaned_line)






    


    

