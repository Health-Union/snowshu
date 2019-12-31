## docker image is intended for development environments only.
## to use snowshu in production install the package via pip
FROM python:3.7-slim-buster

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN apt-get update && \
apt-get install -y vim nano htop screen && \
echo "export PYTHONPATH=${PWD}:$PYTHONPATH" >> /root/.bashrc && \
pip3 install -r requirements/dev.txt
