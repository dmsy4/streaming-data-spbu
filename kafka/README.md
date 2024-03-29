# Задание

С помощью какой-либо клиентской библиотеки (например [confluent-kafka](https://pypi.org/project/confluent-kafka/), [kafka-python](https://pypi.org/project/kafka-python/), [aiokafka](https://pypi.org/project/aiokafka/) или [faust](https://pypi.org/project/faust-streaming/)) проэмулируйте процесс работы датчиков IoT устройств.

В отдельном скрипте работают "производители":
- датчики (их число настраивается) с определенными задержками пишут в один топик показания, каждый согласно какому-то настаиваемому распределению с настраиваемыми задержками по времени. Сообщения пишутся в произвольном формате, должны быть следующие поля: время события, "тип" датчика (температура, давление, и т.п.), имя датчика, значение (число с плавающей точкой). 

В другом скрипте работают "потребители":
- данные накапливаются и каждые 20 секунд (настраиваемый параметр) строятся два pandas DataFrame - среднее значение показаний по типу датчика и по имени датчика. Данные выводятся в консоль.

# Запуск

Запускаем Kafka брокер, порт 9092

```bash
$ docker compose up
```

В отдельном терминале запускаем скрипт с producer, например

```python
python3 producer.py --num-sensors 2 --delays 0.1 0.1 --sensors-values-mean 2 10 --sensors-values-std 2 3
```

В отдельном терминале собираем информацию с помощью consumer
```python
python3 consumer.py --interval 20
```

Пример выводимой информации для команд выше:

```
----------------------------------------------------------------------
Time: 2023-12-18 21:04:20.085419, collected 119 sensor records
Statistics by Sensor Type:
                value
type                 
humidity     5.714359
pressure     5.677297
temperature  5.931395
--------------------------------------------------
Statistics by Sensor Name:
              value
name               
sensor_0   1.535500
sensor_1  10.098983
----------------------------------------------------------------------
----------------------------------------------------------------------
Time: 2023-12-18 21:04:40.045466, collected 517 sensor records
Statistics by Sensor Type:
                value
type                 
humidity     5.562426
pressure     6.107733
temperature  6.173125
--------------------------------------------------
Statistics by Sensor Name:
              value
name               
sensor_0   1.874517
sensor_1  10.044767
----------------------------------------------------------------------
```