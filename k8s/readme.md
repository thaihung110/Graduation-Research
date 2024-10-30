# Deploy airflow on k8s 

## Install kubernetes 
Step 1. Install kubectl 
https://kubernetes.io/vi/docs/tasks/tools/install-kubectl/#install-kubectl-on-macos
Step 2. Install minikube
https://kubernetes.io/vi/docs/tasks/tools/install-minikube/

After installing minikube, start with command
```
minikube start
```
Check minikube status by command
```
minikube status
```
## Installing the chart

Install the latest image of Airflow
```
helm repo add apache-airflow https://airflow.apache.org 
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```

Check the pods and application are up and running
```
➜ kubectl get pods -n airflow

NAME                                 READY   STATUS    RESTARTS   AGE
airflow-postgresql-0                 1/1     Running   0          3m39s
airflow-redis-0                      1/1     Running   0          3m39s
airflow-scheduler-77b484b6d5-hhpvj   2/2     Running   0          3m39s
airflow-statsd-7d985bcb6f-q2qfq      1/1     Running   0          3m39s
airflow-triggerer-0                  2/2     Running   0          3m39s
airflow-webserver-5b5cc47fc-68t8p    1/1     Running   0          3m39s
airflow-worker-0                     2/2     Running   0          3m39s

➜  helm ls -n airflow

NAME    NAMESPACE       REVISION        UPDATED                                 STATUS          CHART           APP VERSION
airflow airflow         1               2023-11-21 15:39:00.208869 +0400 +04    deployed        airflow-1.11.0  2.7.1   
```

To access the Airflow UI in our browser:
```
➜  kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow

Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

Move to configuration folder, export values to a local file
```
helm show values apache-airflow/airflow > values.yaml
```

Open file ```values.yaml``` and change **gitSync** settings:

```
gitSync:
    enabled: true

    repo: https://github.com/thaihung110/Graduation-Research.git
    branch: anmd
    rev: HEAD

    ref: anmd
    depth: 1

    maxFailures: 0

    subPath: "airflow/dags"

    wait: 5

    containerName: git-sync
    uid: 65533

    securityContext: {}
    securityContexts:
      container: {}
    containerLifecycleHooks: {}
    extraVolumeMounts: []
    env: []

    resources: {}
    #  limits:
    #   cpu: 100m
    #   memory: 128Mi
    #  requests:
    #   cpu: 100m
    #   memory: 128Mi

```
Upgrade local deployment by using command

```
helm upgrade --install airflow apache-airflow/airflow -n airflow -f values.yaml --debug
```

Rerun ```kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow``` to access the Airflow UI