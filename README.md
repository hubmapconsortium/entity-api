# entity-api for HuBMAP
A set of web service calls to return information about HuBMAP entities.

The webservices are accessible through the `/entities` restful endpoint.
A description of the API calls is found here: [Entities API](http://smart-api.info/ui/12af775769ba65a684476960f5f87e72).

## Development and deployment environments

We have the following 4 development and deployment environments:

* localhost - all the services will be deployed with docker containers including sample Neo4j and sample MySQL are running on the same localhost listing on different ports, without globus data
* dev - all services except ingest-api will be running on AWS EC2 with SSL certificates, Neo4j and MySQL are dev versions on AWS, and ingest-api(and another nginx) will be running on PSC with domain and globus data
* test - similar to dev but for production-like settings with Neo4j and MySQL test versions of database
* prod - similar to test but for production settings with production versions of Neo4j and MySQL

## Flask app configuration

This application is written in Flask and it includes an **app.cfg.example** file in the `/src/instance` directory.  Copy the file and rename it **app.cfg** and modify  with the appropriate information.

## Local development

This assumes you are developing the code with the Flask development server and you have access to the remote neo4j database.

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

## Deploy with other HuBMAP docker compose projects on dev, test, and prod

This option allows you to setup all the pieces in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building the Entity API docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#workflow-of-setting-up-multiple-hubmap-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI.  Modifying the entity-api-spec.yaml file and commititng the changes to github should update the API shown on SmartAPI.  SmartAPI allows users to register API documents.  The Entity-API's are associated with this github account: api-developers@hubmapconsortium.org.  Please contact Chuck Borromeo (chb69@pitt.edu) if you want to register a new API on SmartAPI.
