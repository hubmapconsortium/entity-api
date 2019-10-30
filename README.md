# entity-api for HuBMAP
A set of web service calls to return information about HuBMAP entities.

The webservices are accessible through the /entities restful endpoint.
A description of the API calls is found here: [Entities API](https://drive.google.com/open?id=16C5vINOV53mKO5hIpFycbSdETsi6SIYd2FzB4Py2jBI).  (You may need to ask for permission to view this document)

## Local standalone development

This assumes you are developing the code with the Flask development server and you have access to the remote neo4j database.

### Flask app configuration

This application is written in Flask and it includes an **app.cfg.example** file in the `/src/instance` directory.  Copy the file and rename it **app.cfg** and modify  with the appropriate information.

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

## Local testing against HuBMAP Gateway in a containerized environment

This option allows you to setup all the pieces (gateway for authentication, neo4j database server...) in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building the Entity API docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#share-containers-across-multiple-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

### Required tools

- [Docker](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Note: Docker Compose requires Docker to be installed and running first.


### Flask app configuration

Same as the configuration for the standalone local development, you'll need to rename the **app.cfg.example** file in the `/src/instance` directory to **app.cfg** and modify  with the appropriate information.


### Build docker image

````
cd docker
./docker-setup.sh
sudo docker-compose -f docker-compose.yml -f docker-compose.dev.yml build
````

### Start up container

````
sudo docker-compose -p entity-api_and_neo4j -f docker-compose.yml -f docker-compose.dev.yml up -d
````

Note: here we specify the docker compose project with the `-p` to avoid "WARNING: Found orphan containers ..." due to the fact that docker compose uses the directory name as the default project name.

Also note that the Gateway project creates a network named **gateway_hubmap** and this network will be used by Entity API so the containers can communicate to each other across multiple docker compose projects.