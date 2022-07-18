FROM python:3.9

RUN apt-get update && apt-get -y upgrade

WORKDIR /app

COPY ./ .
COPY ./requirements/ ./requirements/

RUN pip install -r ./requirements/install.pip
RUN pip install -e .
RUN pip install werkzeug==1.0.0

CMD ["csvapi", "serve", "-h", "0.0.0.0", "-p", "8089"]
EXPOSE 8089
