FROM python:latest

WORKDIR /app

RUN apt-get update && apt-get upgrade -y
RUN python -m ensurepip --upgrade
RUN pip install polars

COPY ./src /app/src
WORKDIR /app/src

EXPOSE 80
CMD ["/bin/bash"]
