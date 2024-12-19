chạy docker file trong folder airflow

cd airflow/...
docker build -t airflow:latest .


cd test/..
chạy docker compose
 
docker compose up


tạo tài khoản admin cho airflow ui

docker exec -it demo-airflow-1 airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin


druid: localhost:8888
kafka-ui: localhost:9092


link nối kafka + druid
https://duynguyenngoc.com/posts/real-time-analytics-airflow-kafka-druid-superset/


lưu ý khi nối kafka vào druid thì: 
- bootstrap-server: `host.docker.internal:9092`

