FROM python:3.9-slim

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ENV https_proxy=""
ENV http_proxy=""
ENV no_proxy="127.0.0.1,localhost"

ADD Pipfile* ./
RUN pip install --no-cache-dir httpie pipenv
RUN pipenv install --system --deploy --ignore-pipfile

ENV https_proxy=""
ENV http_proxy=""

ADD . .

ENTRYPOINT ["python3"]

CMD ["main.py"]
