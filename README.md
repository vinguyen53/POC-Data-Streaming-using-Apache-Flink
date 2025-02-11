![flink](https://github.com/user-attachments/assets/9e49c418-59e3-4413-9262-05549fb9c0d4)
### Architecture
- Using Redpanda as pub/sub message queue (compatible with Kafka)
- Data Producer generate data and push to Data Source topic in Redpanda
- Flink read data from Data Source topic, transform and push to Transformed Data topic in Redpanda
- Using Kafka Connect get data from Transformed Data topic and write to ClickHouse (Data Warehouse)
### 1. Build PyFlink image
- At pyflink folder, run `docker build -t pyflink:latest .`
### 2. Download ClickHouse-Kafka Connector
- Download ClickHouse-Kafka self-hosted connector at link https://www.confluent.io/hub/clickhouse/clickhouse-kafka-connect
- Create folder /kafka-connect-plugins and store connector jar file in this folder
### 3. Run docker compose
- At docker compose file directory, run `docker compose up -d`
- Check all services status `docker ps`
### 4. Run data producer
- Run file `kafka_event_producer.ipynb`
- Using Redpanda console at `localhost:8083`, check data in topic `orders`
### 5. Run Pyflink job
- Access to Flink master `docker exec -it <container id> bash`
- Go to home directory `cd /home`
- Run pyflink job `python kafka_flink.py`
- Check transformed data in topic `transformed_orders` using Redpanda console
### 6. Write data to ClickHouse
- Access to ClickHouse `docker exec -it <container id> clickhouse-client`, enter password at `CLICKHOUSE_ADMIN_PASSWORD` in docker compose file
- Run SQL command in init.sql file
- Call POST request to `http://localhost:8084/connectors` with body in `clickhouse-kafka-connect-config.json`, a connector between `transformed_orders` topic and ClickHouse will be generated
