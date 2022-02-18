FROM python:3.7
COPY . /code
WORKDIR /code
RUN apt-get update && apt-get -y install cron
RUN pip install -r requirements.txt
RUN crontab crontab
CMD ["python", "scheduler.py"]