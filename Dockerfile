FROM python:3.9.15-slim-buster

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository non-free
RUN apt-get update && apt-get install -y unrar
ADD build/requirements.txt /REQUIREMENTS.txt
RUN pip install -r /REQUIREMENTS.txt

ADD build/ /usr/local/ais_i_mov
#RUN cd /usr/local/data && unzip \*.zip
WORKDIR /usr/local/
RUN export set PYTHONPATH=$PYTHONPATH:.
RUN mkdir -p /data