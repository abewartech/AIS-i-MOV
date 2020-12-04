FROM python:3.7-buster

ADD ais_i_mov/requirements.txt /REQUIREMENTS.txt
RUN pip install -r /REQUIREMENTS.txt

ADD ais_i_mov/ /usr/local/ais_i_mov
#RUN cd /usr/local/data && unzip \*.zip
WORKDIR /usr/local/
RUN export set PYTHONPATH=$PYTHONPATH:.
