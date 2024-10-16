# etl_anp

Este projeto é um Data Pipeline gerenciado pelo Apache Airflow. O ambiente está configurado para funcionar a partir do Docker.

O arquivo `docker-compose.yaml` utilizado foi o sugerido pelo Quick Start da documentação do Apache Airflow.

A instalação dos pacotes de idioma `pt_BR` foi a única customização necessária na imagem. Para isso, segui a sugestão que se encontra na linha #44 do `docker-compose.yaml` baixado de:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
