# entity-api for HuBMAP
A set of web service calls to return information about HuBMAP entities.

The webservices are accessible through the /entities restful endpoint.
A description of the API calls is found here: [Entities API](http://smart-api.info/ui/12af775769ba65a684476960f5f87e72).

## Local development

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

## Local testing against HuBMAP Gateway

This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally.

### Overview of tools

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
