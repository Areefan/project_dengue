FROM python:3.9.7
 
RUN apt-get update
RUN apt-get install -y wget cron

WORKDIR /dengue_api_cronjob

ADD . /dengue_api_cronjob

# COPY crontab /etc/cron.d/crontab
# RUN crontab /etc/cron.d/crontab

RUN pip install --no-cache-dir -r requirements.txt

CMD fastapi run main.py --port 80