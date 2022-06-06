# HuBMAP Entity API

A set of standard RESTful web service that provides CRUD operations into our entity metadata store. A description of the API calls is found here: [Entities API](https://smart-api.info/ui/0065e419668f3336a40d1f5ab89c6ba3).

## Entities schema yaml

The yaml file `src/resources/hubmap-entities.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.


## Overview of tools

- [Docker Engine](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Note: Docker Compose requires Docker to be installed and running first.

### Docker post-installation configurations

The Docker daemon binds to a Unix socket instead of a TCP port. By default that Unix socket is owned by the user root and other users can only access it using sudo. The Docker daemon always runs as the root user. If you don’t want to preface the docker command with sudo, add users to the `docker` group:

````
sudo usermod -aG docker $USER
````

Then log out and log back in so that your group membership is re-evaluated. If testing on a virtual machine, it may be necessary to restart the virtual machine for changes to take effect.

Note: the following instructions with docker commands are based on managing Docker as a non-root user.


## Docker build for local/DEV development

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation when we need to use a branch of commons from github rather than the published PyPI package. Default to master branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1000 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1000 if not set or null.

We can set and verify the environment variable like below:

````
export COMMONS_BRANCH=master
echo $COMMONS_BRANCH
````

Note: Environment variables set like this are only stored temporally. When you exit the running instance of bash by exiting the terminal, they get discarded. So for rebuilding the docker image, we'll need to make sure to set the environment variables again if necessary.

```
cd docker
./docker-development.sh [check|config|build|start|stop|down]
```

## Docker build for deployment on TEST/STAGE/PROD

```
cd docker
export ENTITY_API_VERSION=a.b.c (replace with the actual released version number)
./docker-deployment.sh [start|stop|down]
```

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI. Modifying the `entity-api-spec.yaml` file and commititng the changes to github should update the API shown on SmartAPI. SmartAPI allows users to register API documents.  The documentation is associated with this github account: api-developers@hubmapconsortium.org.
