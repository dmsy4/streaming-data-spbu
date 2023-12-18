import argparse
import json
import time
import threading
from random import gauss, choice
from confluent_kafka import Producer


def generate_sensor_data(sensor_id, mean, std):
    sensor_type = choice(['temperature', 'pressure', 'humidity'])
    sensor_name = f"sensor_{sensor_id}"
    value = round(gauss(mean, std), 2)
    timestamp = int(time.time())

    return {
        'timestamp': timestamp,
        'sensor_type': sensor_type,
        'sensor_name': sensor_name,
        'value': value
    }

def produce_message(producer, topic, sensor_id, delay, mean, std, run_time):
    start_time = time.time()
    while True:
        sensor_data = generate_sensor_data(sensor_id, mean, std)
        producer.produce(topic, value=json.dumps(sensor_data))
        producer.poll(0)
        time.sleep(delay)

        if (time.time() - start_time) >= run_time:
            break

def produce_messages(bootstrap_servers, topic, num_sensors, delays, means, stds, run_time):
    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    threads = []
    for i in range(num_sensors):
        thread = threading.Thread(
            target=produce_message,
            args=(
                producer,
                topic,
                i,
                delays[i],
                means[i],
                stds[i],
                run_time
            )
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Kafka producer for IoT sensor data")
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
        '--num-sensors',
        type=int,
        help='Number of sensors'
    )
    parser.add_argument(
        '--delays',
        type=float,
        nargs='+',
        help='Time delay between sensor readings in seconds'
    )
    parser.add_argument(
        '--sensors-values-mean',
        type=float,
        nargs='+',
        help='Mean value of sensor values'
    )
    parser.add_argument(
        '--sensors-values-std',
        type=float,
        nargs='+',
        help='Standard deviation of sensor values'
    )
    parser.add_argument(
        '--run-time',
        type=float,
        default=None,
        help='Total running time in seconds. If not specified, runs indefinitely.'
    )

    args = parser.parse_args()
    if args.run_time is None:
        args.run_time = float("inf")

    if len(args.delays) != args.num_sensors \
        or len(args.sensors_values_mean) != args.num_sensors \
        or len(args.sensors_values_std) != args.num_sensors:
        print("Error: Number of elements in delays/mean/std args must match the number of sensors.")
    else:
        produce_messages(
            args.bootstrap_servers,
            args.topic,
            args.num_sensors,
            args.delays,
            args.sensors_values_mean,
            args.sensors_values_std,
            args.run_time
        )


if __name__ == '__main__':
    main()
