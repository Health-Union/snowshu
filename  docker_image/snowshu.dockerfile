FROM python:3.7

RUN pip3 install snowshu>=0.0.3 && \
mkdir /workspace

WORKDIR /workspace

COPY ./entrypoint.sh /workspace/entrypoint.sh

ENTRYPOINT ["/workspace/entrypoint.sh"]
