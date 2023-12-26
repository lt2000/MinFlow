# install docker
sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
# install and initialize couchdb
sudo docker pull couchdb
sudo docker run -itd -p 5984:5984 -e COUCHDB_USER=little -e COUCHDB_PASSWORD=little --name couchdb couchdb
python3 ~/minflow/scripts/couchdb_starter.py

# # install redis
sudo docker pull redis
sudo docker network create --driver bridge --subnet 172.16.0.1/16 --gateway 172.16.0.1 docker1
sudo docker run -itd -p 8002:6379 --name redis  --network docker1 redis

