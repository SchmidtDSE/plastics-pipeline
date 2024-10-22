FROM ubuntu:jammy-20230916

RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install -y python-is-python3
RUN apt-get install -y sqlite3
RUN apt-get install -y zip
RUN apt-get install -y wget
RUN apt-get install -y wget2

RUN mkdir /workspace
WORKDIR /workspace

COPY . .

RUN pip install -r requirements.txt

WORKDIR /workspace/data

RUN bash get_data.sh
