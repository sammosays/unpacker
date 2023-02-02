#!/usr/bin/python3

import boto3
import pika

import json
import os
import sys

MINIO_TOKEN_ENV_VAR = 'MINIO_ACCESS_TOKEN'
MINIO_SECRET_ENV_VAR = 'MINIO_ACCESS_SECRET'
NOTIFY_RECEIVE_QUEUE = 'unpacker-queue'
NOTIFY_SEND_QUEUE = ''


def load_env_var(name):
    try:
        env_var = os.environ[name]
        return env_var
    except KeyError:
        print(f'missing {name} environment variable')
        sys.exit(0)


MINIO_ACCESS_TOKEN = load_env_var(MINIO_TOKEN_ENV_VAR)
MINIO_ACCESS_SECRET = load_env_var(MINIO_SECRET_ENV_VAR)

session = boto3.resource(
    's3',
    endpoint_url='https://minio:9000',
    aws_access_key_id=MINIO_ACCESS_TOKEN,
    aws_secret_access_key=MINIO_ACCESS_SECRET,
    verify=False
)
s3 = session.resource('s3')


def callback_download(ch, method, properties, body):
    print(f'received: {body}')
    event = json.loads(body.decode())
    if 'EventName' in event and event['EventName'] == 's3:ObjectCreated:Put':
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            obj = s3.get_object(Bucket=bucket, Key=key)
            print(f'{obj}')


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue=NOTIFY_RECEIVE_QUEUE)
    channel.basic_consume(queue=NOTIFY_RECEIVE_QUEUE,
                          auto_ack=True,
                          on_message_callback=callback_download)

    print('listening for notifications..')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('shutting down..')
        sys.exit(0)
