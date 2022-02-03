FROM python:3.8

RUN pip3 install snowshu && \
mkdir /workspace

WORKDIR /workspace

COPY ./entrypoint.sh /workspace/entrypoint.sh

ENTRYPOINT ["/workspace/entrypoint.sh"]
