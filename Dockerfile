# Dockerfile

FROM python:3.10-slim

RUN apt-get update && apt-get install -y openjdk-17-jre-headless wget

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install pyspark==3.5.0 delta-spark==3.2.0 prefect==2.14.12 griffe==0.32.1

WORKDIR /app

COPY . .

CMD ["python", "medallion-prefect.py"]
