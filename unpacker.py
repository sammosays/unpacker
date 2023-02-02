#!/usr/bin/python3

import boto3
import pika

import io
import json
import os
import sys
from zipfile import ZipFile


MINIO_TOKEN_ENV_VAR = 'MINIO_ACCESS_TOKEN'
MINIO_SECRET_ENV_VAR = 'MINIO_ACCESS_SECRET'
MINIO_URL = 'http://minio-service:9000'
NOTIFY_RECEIVE_QUEUE = 'unpacker-queue'
PUBLISH_EXCHANGE = 'dw'
PUBLISH_ROUTING_KEY = 'formatter-queue'
UNPACKED_BUCKET = 'unpacked'


def load_env_var(name):
    try:
        env_var = os.environ[name]
        return env_var
    except KeyError:
        print(f'missing {name} environment variable')
        sys.exit(0)


MINIO_ACCESS_TOKEN = load_env_var(MINIO_TOKEN_ENV_VAR)
MINIO_ACCESS_SECRET = load_env_var(MINIO_SECRET_ENV_VAR)

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_ACCESS_TOKEN,
    aws_secret_access_key=MINIO_ACCESS_SECRET,
    verify=False
)


def publish_event(event):
    global PUBLISH_EXCHANGE
    global PUBLISH_ROUTING_KEY
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.basic_publish(PUBLISH_EXCHANGE,
                              PUBLISH_ROUTING_KEY,
                              event,
                              pika.BasicProperties(content_type='text/plain',
                                                   delivery_mode=pika.DeliveryMode.Transient))
        print(f'published event to exchange [{PUBLISH_EXCHANGE}] routing key: [{PUBLISH_ROUTING_KEY}]')
        connection.close()
    except Exception as e:
        print(f'failed to publish event to broker - {event} - {e}')


def callback(ch, method, properties, body):
    global s3
    global UNPACKED_BUCKET

    # receive event from rabbit
    print(f'received: {body}')
    event = json.loads(body.decode())
    records = []

    # process each record in the event
    if 'EventName' in event and event['EventName'] == 's3:ObjectCreated:Put':
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            try:
                # download object as a zip
                obj = s3.get_object(Bucket=bucket, Key=key)
                zip_file = ZipFile(io.BytesIO(obj['Body'].read()))
                print(f'downloaded {key} from {bucket}')

                # unzip each file in memory
                unpacked = []
                for file_name in zip_file.namelist():
                    file = zip_file.open(file_name).read()
                    print(f'extracted {file_name} from zip {key}')

                    # post file to minio and add to record
                    s3.put_object(Body=file, Bucket=UNPACKED_BUCKET, Key=file_name)
                    unpacked.append({'key': file_name, 'bucket': UNPACKED_BUCKET})
                    print(f'posted {file_name} to bucket {UNPACKED_BUCKET}')

                # update event
                record['unpacked'] = unpacked
                records.append(record)

            except Exception as e:
                print(f'error processing key [{key}] from bucket [{bucket}] - {e}')

        # publish event to rabbit
        event['Records'] = records
        publish_event(event)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue=NOTIFY_RECEIVE_QUEUE)
    channel.basic_consume(queue=NOTIFY_RECEIVE_QUEUE,
                          auto_ack=True,
                          on_message_callback=callback)

    print('listening for notifications..')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('shutting down..')
        sys.exit(0)
