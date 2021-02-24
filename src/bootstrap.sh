#!/bin/bash

sudo python3 -m pip install \
    botocore \
    boto3 \
    ujson \
    warcio \
    tabulate

sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install --upgrade pip setuptools
sudo python3 -m pip install wheel
sudo python3 -m pip install -U Cython
sudo python3 -m pip install -U spacy==2.3.5
sudo python3 -m spacy download en_core_web_lg
sudo python3 -m pip install torch torchvision
sudo python3 -m pip install numpy==1.18.0
sudo python3 -m pip install tensorflow
sudo python3 -m pip install ktrain==0.19.3
sudo python3 -m pip install pandas==1.0.4
sudo python3 -m pip install pytorch_pretrained_bert==0.6.2
sudo python3 -m pip install simpletransformers==0.45.0