# As Scrapy runs on Python, I choose the official Python 3 Docker image.
FROM python:3
 
# Set the working directory to /usr/src/app.
WORKDIR /app
 
# Copy the file from the local host to the filesystem of the container at the working directory.
COPY ./app/requirements.txt /app
 
# Install Scrapy specified in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy the project source code from the local host to the filesystem of the container at the working directory.
COPY ./app /app
 
RUN chmod +x run.sh
# Run the crawler when the container launches.
CMD ["sh", "run.sh"]