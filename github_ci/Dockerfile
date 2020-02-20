FROM debian:bullseye-slim

RUN mkdir /app && \
apt-get update && \
apt-get install -y git 

WORKDIR /app

RUN git config --global user.name "sync bot" && \
 git config --global user.email "data@health-union.com" && \
git clone git@github.com:Health-Union/snowshu.git && \
git push https://ethanknox:${BB_API_TOKEN}@bitbucket.org/healthunion/snowshu.git

