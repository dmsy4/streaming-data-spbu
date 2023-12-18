import argparse
import json
import pandas as pd
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError


def consume_messages(consumer, interval):
    collected_data = {
        'by_type': {'type': [], 'value': []},
        'by_name': {'name': [], 'value': []},
    }
    last_print_time = None
    while True:
        msg = consumer.poll(1)

        if not msg:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        data = json.loads(msg.value().decode('utf-8'))
        sensor_name, sensor_type, sensor_value = data['sensor_name'], data['sensor_type'], data['value']
        collected_data["by_type"]["type"].append(sensor_type)
        collected_data["by_type"]["value"].append(sensor_value)

        collected_data["by_name"]["name"].append(sensor_name)
        collected_data["by_name"]["value"].append(sensor_value)

        # Печатаем данные каждые 20 секунд
        if int(time.time()) % interval == 0 and int(time.time()) != last_print_time:
            df_type = pd.DataFrame(collected_data["by_type"])
            df_name = pd.DataFrame(collected_data["by_name"])
            
            print('-' * 70)
            print(f"Time: {str(datetime.utcnow())}, collected {len(collected_data['by_type']['type'])} sensor records")
            print("Statistics by Sensor Type:")
            print(df_type.groupby('type').mean())
            print('-' * 50)
            print("Statistics by Sensor Name:")
            print(df_name.groupby('name').mean())
            print('-' * 70)
            last_print_time = int(time.time())


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer for IoT sensor data")
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default='sensors_data',
        help='Kafka topic for sensor data'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=20,
        help='Interval in seconds for printing DataFrames'
    )

    args = parser.parse_args()

    consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': 'iot_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.topic])

    try:
        consume_messages(consumer, args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
