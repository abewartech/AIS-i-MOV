FROM python:3.9.15-slim-buster

ADD build/requirements.txt /REQUIREMENTS.txt
RUN pip install -r /REQUIREMENTS.txt

ADD build/ /usr/local/ais_i_mov
#RUN cd /usr/local/data && unzip \*.zip
WORKDIR /usr/local/
RUN export set PYTHONPATH=$PYTHONPATH:.
RUN mkdir -p ais_i_mov/data