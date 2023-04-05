FROM python:3.8
ADD . /producer
WORKDIR /producer
RUN pip install -r requirements.txt

CMD ["python3", "-u", "openweather_producer.py"]
