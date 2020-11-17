# entity-api for HuBMAP

A set of web service calls to return information about HuBMAP entities.

A description of the API calls is found here: [Entities API](http://smart-api.info/ui/12af775769ba65a684476960f5f87e72).

## Entities schema yaml

The yaml file `src/schema/hubmap-entities.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.

## API endpoints and examples

### Get all entity classes

````
GET https://entity-api.refactor.hubmapconsortium.org/entity_classes
````
This returns a list of normalized entity classes: Collection, Dataset, Donor, Sample

### Get an entity by id

````
GET https://entity-api.refactor.hubmapconsortium.org/entities/<id>
````

Note: The `<id>` can be either a HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity

Result filtering is supported based on query string. For example:

````
GET https://entity-api.refactor.hubmapconsortium.org/entities/<id>?property=data_access_level
````

### Get all entities of a given class 

````
GET https://entity-api.refactor.hubmapconsortium.org/<entity_class>/entities
````

Result filtering is supported based on query string. For example:

````
GET https://entity-api.refactor.hubmapconsortium.org/<entity_class>/entities?property=uuid
````

This would return a list of UUIDs of the given entity class instead of all the properties.

### Create an entity (new or derived) of the target class

````
POST https://entity-api.refactor.hubmapconsortium.org/entities/<entity_class>
````

The same JSON envelop will be used in the request body: 

Create a new entity:

````
{
    "source_entities": null or [],
    "target_entity": {
        all the standard properties defined in schema yaml for the target class...
    }
}
````

Create a derived entity:

````
{
    "source_entities": [
        {"class": "Sample", "id": "44324234"},
        {"class": "Sample", "id": "6adsd230"},
        ...
    ],
    "target_entity": {
        all the standard properties defined in schema yaml for the target class...
    }
}
````

### Update the properties of a given entity (except for Collection)

````
PUT https://entity-api.refactor.hubmapconsortium.org/entities/<id>
````

The JSON request body will need to contain the properties (only the ones to be updated) defiend in the schema yaml file.

### Get all the ancestors of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/ancestors/<id>
````


### Get all the descendants of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/descendants/<id>
````


### Get all the parents of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/parents/<id>
````


### Get all the children of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/children/<id>
````

### Get the Globus URL to the given dataset

````
GET https://entity-api.refactor.hubmapconsortium.org/dataset/globus-url/<id>
````

### Redirect a request from a doi service for a collection of data

````
GET https://entity-api.refactor.hubmapconsortium.org/collection/redirect/<id>
````

## Development and deployment environments

We have the following 5 development and deployment environments:

* localhost - all the services will be deployed with docker containers including sample Neo4j and sample MySQL are running on the same localhost listing on different ports, without globus data
* dev - all services except ingest-api will be running on AWS EC2 with SSL certificates, Neo4j and MySQL are dev versions on AWS, and ingest-api(and another nginx) will be running on PSC with domain and globus data
* test - similar to dev with a focus on testing and connects to Neo4j and MySQL test versions of database
* stage - as similar to the production environment as it can be.
* prod - similar to test but for production settings with production versions of Neo4j and MySQL

### Localhost development

This option allows you to setup all the pieces in a containerized environment with docker and docker-compose. This requires to have the [HuBMAP Gateway](https://github.com/hubmapconsortium/gateway) running locally before starting building this docker compose project. Please follow the [instructions](https://github.com/hubmapconsortium/gateway#workflow-of-setting-up-multiple-hubmap-docker-compose-projects). It also requires the Gateway project to be configured accordingly.

### Remote deployment

In localhost mode, all the docker containers are running on the same host machine. However, the ingest-api will be deployed on a separare host machine for dev, test, stage, and prod mode due to different deployment requirements. 

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation. We can specify which [commons](https://github.com/hubmapconsortium/commons) branch to use during the image creation. Default to master branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1000 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1000 if not set or null.

We can set and verify the environment variable like below:

````
export COMMONS_BRANCH=devel
echo $COMMONS_BRANCH
````

Note: Environment variables set like this are only stored temporally. When you exit the running instance of bash by exiting the terminal, they get discarded. So for rebuilding the docker image, we'll need to make sure to set the environment variables again if necessary.

````
Usage: ./entity-api-docker.sh [localhost|dev|test|stage|prod] [check|config|build|start|stop|down]
````

Before we go ahead to start building the docker image, we can do a check to see if the required configuration file is in place:

````
cd docker
./entity-api-docker.sh dev check
````

We can also validate and view the details of corresponding compose file:

````
./entity-api-docker.sh dev config
````

Building the docker images and starting/stopping the contianers require to use docker daemon, you'll probably need to use `sudo` in the following steps. If you don’t want to preface the docker command with sudo, add users to the docker group:

````
sudo usermod -aG docker $USER
````

Then log out and log back in so that your group membership is re-evaluated. If testing on a virtual machine, it may be necessary to restart the virtual machine for changes to take effect.

To build the docker image of entity-api:

````
./entity-api-docker.sh dev build
````

To start up the entity-api container:

````
./entity-api-docker.sh dev start
````

And stop the running container by:

````
./entity-api-docker.sh dev stop
````

You can also stop the running container and remove it by:

````
./entity-api-docker.sh dev down
````

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI.  Modifying the `entity-api-spec.yaml` file and commititng the changes to github should update the API shown on SmartAPI.  SmartAPI allows users to register API documents.  The documentation is associated with this github account: api-developers@hubmapconsortium.org. Please contact Chuck Borromeo (chb69@pitt.edu) if you want to register a new API on SmartAPI.
