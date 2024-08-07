FROM python:3.9.7
 
RUN apt-get update
RUN apt-get install -y wget cron

WORKDIR /r506_pipeline_cronjob

ADD . /r506_pipeline_cronjob

COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN pip install --no-cache-dir -r requirements.txt

CMD python main.py

# FROM python:3.9.7
# WORKDIR /usr/src/app
# RUN apt-get update && apt-get install -y \
# 	libsasl2-dev \
# 	python-dev \
# 	libldap2-dev \
# 	libssl-dev
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . .
# RUN groupadd -r python && useradd -r -g python python
# RUN apt-get update \ 
# 	&& apt-get install -y apt-transport-https \
# 	&& apt-get install mariadb-client -y
# USER python

# CMD fastapi run main.py --port 80
# CMD ["fastapi", "run", "main.py", "--port", "80"]

# FROM python:3.12-slim