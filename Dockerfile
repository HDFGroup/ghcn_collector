# syntax=docker/dockerfile:1.2

FROM python:3.9-slim-bullseye

RUN apt update \
 && apt install -y git \
 && pip install pyyaml \
 && pip install h5pyd \
 && pip install h5py \
 && pip install boto3

COPY config.py /ghcn_collector
COPY ghcn_dtype.py /ghcn_collector
COPY ghcn_update.py /ghcn_collector

WORKDIR /ghcn_collector

ENTRYPOINT ["python", "-u", "ghcn_update.py"]
