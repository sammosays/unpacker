FROM python:3.9-alpine
RUN apk update && apk add --no-cache curl vim bash iputils

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY *.py ./

RUN chmod 777 -R /usr/src/app

CMD ["python3", "-u", "unpacker.py"]