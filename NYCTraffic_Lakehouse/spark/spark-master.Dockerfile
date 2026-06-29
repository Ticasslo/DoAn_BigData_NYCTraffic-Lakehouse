FROM spark:4.0.0-scala2.13-java21-python3-ubuntu

USER root
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y python3.13 && \
    ln -sf /usr/bin/python3.13 /usr/bin/python3

USER spark