# Use an official Python runtime as a parent image
FROM python:3.9.7

# Set the working directory to /app
WORKDIR /app

# Install Jupyter Notebook
RUN pip install jupyter

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8888 available to the world outside this container
EXPOSE 8888

# Define environment variable
# ENV NAME World

# Run Jupyter Notebook when the container launches
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

# FROM python:3.9.7
 
# RUN apt-get update
# RUN apt-get install -y wget cron

# WORKDIR /dengue_model_cronjob

# ADD . /dengue_model_cronjob

# COPY crontab /etc/cron.d/crontab
# RUN crontab /etc/cron.d/crontab

# RUN pip install --no-cache-dir -r requirements.txt

# CMD python main.py