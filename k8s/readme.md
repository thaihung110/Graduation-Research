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

```
helm repo add apache-airflow https://airflow.apache.org 
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```

```
kubectl get pods -n airflow

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

```
➜  kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow

Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

