# FROM python:3.10.15-alpine3.20
FROM python:3.10.15-slim

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

LABEL Maintainer="wisrovi.rodriguez@gmail.com"

RUN apt update -y
RUN apt-get install -y libgl1
RUN apt-get install -y libglib2.0-0

WORKDIR /tmp

# install libreries for testing
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt



WORKDIR /app

COPY ./ /app

CMD ["python", "basic_web.py"]

