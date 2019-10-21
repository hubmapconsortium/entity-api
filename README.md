# entity-api for HuBMAP
A set of web service calls to return information about HuBMAP entities.

The webservices are accessible through the /entities restful endpoint.
A description of the API calls is found here: [Entities API](https://drive.google.com/open?id=16C5vINOV53mKO5hIpFycbSdETsi6SIYd2FzB4Py2jBI).  (You may need to ask for permission to view this document)

## Local standalone development

This assumes you are developing the code with the Flask development server and you have access to the remote neo4j database.

### Flask config

This application is written in Flask and it includes an **app.properties.example** file in the `/conf` directory.  Copy the file and rename it **app.properties**.  Modify **app.properties** with the appropriate information.

### Install dependencies

````
sudo pip3 install -r src/requirements.txt
````

Note: if you need to use a modified version of the [HuBMAP commons] dependency, download the code and make changes, then install the dependency using `src/requirements_dev.txt` and make sure the local file system path is specified correctly.

### Start Flask development server

````
cd src
export FLASK_APP=app.py
export FLASK_ENV=development
flask run
````

This code runs by default on port 5006. You can change the port using a `-p` or `--port` switch at command line. For instance:

````
flask run -p 5001
````

## Local testing against HuBMAP Gateway in a containeried environment

This option allows you to setup all the pieces (gateway for authentication, neo4j database server...) in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally.

### Required tools

- [Docker](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Note: Docker Compose requires Docker to be installed and running first.


### uWSGI config

In the `Dockerfile`, we installed uWSGI and the uWSGI Python plugin via yum. There's also a uWSGI configuration file at `src/uwsgi.ini` and it tells uWSGI the details of running this Flask app.


### Build docker image

````
sudo docker-compose build
````

### Start up service

````
sudo docker-compose up
````