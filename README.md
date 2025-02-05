![flink](https://github.com/user-attachments/assets/9e49c418-59e3-4413-9262-05549fb9c0d4)
### Architecture
- Using Redpanda as pub/sub message queue (compatible with Kafka)
- Data Producer generate data and push to Data Source topic in Redpanda
- Flink read data from Data Source topic, transform and push to Transformed Data topic in Redpanda
- Using Kafka Connect get data from Transformed Data topic and write to ClickHouse (Data Warehouse)
