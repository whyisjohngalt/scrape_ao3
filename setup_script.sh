#!/bin/sh
apt-get update
apt-get install python3-pip -y
pip install -r reqs.txt
export GOOGLE_APPLICATION_CREDENTIALS=client_secrets.json
exit       
