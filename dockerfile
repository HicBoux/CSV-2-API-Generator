FROM python:3.7-slim

RUN mkdir /csv2api
COPY ./requirements.txt /csv2api
COPY . /csv2api
WORKDIR /csv2api

COPY requirements.txt .

RUN pip3 install -U pip setuptools
RUN pip3 install --no-cache-dir -U -r requirements.txt

EXPOSE 5000

CMD ["python3", "./csv2api.py"]