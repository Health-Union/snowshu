FROM debian:bullseye-slim

RUN mkdir /app && \
apt-get update && \
apt-get install -y git 

WORKDIR /app
COPY ./entrypoint.sh /app/entrypoint.sh
RUN git config --global user.name "sync bot" && \
 git config --global user.email "data@health-union.com" && \
git clone https://github.com/Health-Union/snowshu.git  

WORKDIR /app/snowshu

ENTRYPOINT '/app/entrypoint.sh'



