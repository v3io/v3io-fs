FROM python:3.8-slim
RUN apt-get update && apt-get install -y gcc make
WORKDIR /code
COPY requirements.txt .
COPY dev-requirements.txt .
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt -r dev-requirements.txt
COPY . .
ARG V3IO_API
ARG V3IO_ACCESS_KEY
ENV V3IO_API=$V3IO_API
ENV V3IO_ACCESS_KEY=$V3IO_ACCESS_KEY
RUN make test
