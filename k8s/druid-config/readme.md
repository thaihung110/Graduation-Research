
https://github.com/asdf2014/druid-helm

helm repo add druid-helm https://asdf2014.github.io/druid-helm/

helm upgrade --install my-druid druid-helm/druid --version 31.0.1 --namespace druid --create-namespace

helm upgrade --install my-druid druid-helm/druid -n druid -f values.yaml --debug