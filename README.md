# jupyterlab
Python with Spark env for real time data analysis.
```bash
py -m venv venv
venv\Scripts\activate
pip install --no-cache-dir --upgrade pip setuptools
pip install cookiecutter
cookiecutter https://github.com/jtargalski/RTA_project
cd .cookiecutters\RTA_project
docker compose up
```


```bash
cd jupyterlab-project
docker compose up
```

## stop

```bash
ctrl + c 
docker compose down
```

### Contain: 
- Zookeeper
- Apache Kafka Broker with kafka directory for manipulation from terminal
- MongoDB
- JupyterLab env with Apache Spark

### Python libraries
- tensorflow==2.15.0
- torch==2.0.1
- torchvision==0.15.2
- flask==3.0.1 fastapi==0.115.5 for production env with python ML model 
- pennylane==0.39 - You can use it for quantum machine learning
- pandas==2.0.3 numpy==1.24.4 scipy==1.11.3 matplotlib==3.8.0 sympy==1.12 seaborn==0.13.0
- kafka-python==2.0.2 confluent_kafka==2.6.0
