FROM bitnami/spark:latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3


WORKDIR /opt/application/

COPY . /opt/application/


RUN pip install -r requirements.txt \
    && apt-get update && apt-get install -y \
    curl \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip