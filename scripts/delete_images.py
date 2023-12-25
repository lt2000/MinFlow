import os

with open('./images.txt', 'r') as f:
    images = f.readlines()   

for image in images:
    image = image.strip()
    os.system('docker image rm -f {}'.format(image))
                             