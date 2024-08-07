FROM python:3.9.7
 
RUN apt-get update
RUN apt-get install -y wget cron

WORKDIR /press_pipeline_cronjob

ADD . /press_pipeline_cronjob

COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN pip install --no-cache-dir -r requirements.txt

CMD python main.py

