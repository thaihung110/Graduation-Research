```
docker build -t my-airflow:<version> .
```

```
minikube image load my-airflow:<version>
```

Update airflow ```values.yaml```:

```
# Default airflow repository -- overridden by all the specific images below
defaultAirflowRepository: my-airflow

# Default airflow tag to deploy
defaultAirflowTag: "<version>"
```
