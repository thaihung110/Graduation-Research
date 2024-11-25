### Airflow config
```
docker exec -it graduation-research-airflow-1 airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin
```


### Kafka UI 
web address: `localhost:9092`


### Druid 
Connect druid to consume kafka topic 
- bootstrap-server: `host.docker.internal:9092`
- segment: day
