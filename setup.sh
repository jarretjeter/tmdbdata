!# /bin/bash

sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.8
sudo apt install -y python3-pip
sudo pip3 install azure-core==1.24.0 azure-storage-blob==12.12.0 certifi==2022.12.7 charset-normalizer==3.0.1 click==8.1.3 idna==3.4 numpy==1.24.1 pandas==1.5.3 PyMySQL==1.0.3 python-dateutil==2.8.2 pytz==2022.7.1 requests==2.28.2 six==1.16.0 tmdbsimple==2.9.1 typer==0.4.1 urllib3==1.26.14
echo setup complete
