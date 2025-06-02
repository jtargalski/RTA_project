# jupyterlab
Python with Spark env for real time data analysis.
```bash
py -m venv venv
venv\Scripts\activate
pip install --no-cache-dir --upgrade pip setuptools
pip install cookiecutter
cookiecutter https://github.com/sebkaz/jupyterlab-project
cd .cookiecutters\jupyterlab-project
docker compose up
```
After composing the Docker containers, go to localhost:8080 to use the JupyterLab.

First, run the terminal in the JupyterLab and install the following missing packages for the project:

```bash
pip install pymongo streamlit altair
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
