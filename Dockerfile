# specify start image
FROM python:3.8.0

# all commands start from this directory
WORKDIR /app

# copy all files from this folder to working directory (ignores files in .dockerignore)
COPY . .

RUN pip install -r requirements.txt


# set the start command
CMD [ "python3", "app.py"]