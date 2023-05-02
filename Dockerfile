# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory to /app
#ADD main.py /
ADD . /

#COPY requirements.txt /loka/requirements.txt
COPY requirements.txt ./
#RUN pip install -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Start your application
CMD [ "python", "./main.py" ]
