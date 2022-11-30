# HuBMAP Entity API

A set of standard RESTful web service that provides CRUD operations into our entity metadata store. A description of the API calls is found here: [Entities API](https://smart-api.info/ui/0065e419668f3336a40d1f5ab89c6ba3).

## Entities schema yaml

The yaml file `src/resources/hubmap-entities.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.

## Docker build for local/DEV development

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation when we need to use a branch of commons from github rather than the published PyPI package. Default to main branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1001 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1001 if not set or null.

```
cd docker
./docker-development.sh [check|config|build|start|stop|down]
```

## Docker build for deployment on TEST/STAGE/PROD

```
cd docker
./docker-deployment.sh [start|stop|down]
```

## Development process

### To release via TEST infrastructure
- Make new feature or bug fix branches from `main` branch (the default branch)
- Make PRs to `main`
- As a codeowner, Zhou (github username `yuanzhou`) is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to TEST infrastructure, and redeploy the TEST instance.
- Developer or someone on the team who is familiar with the change will test/qa the change
- When any current changes in the `main` have been approved after test/qa on TEST, Zhou will release to PROD using the same docker image that has been tested on TEST infrastructure.

### To work on features in the development environment before ready for testing and releasing
- Make new feature branches off the `main` branch
- Make PRs to `dev-integrate`
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to devel, and redeploy the DEV instance.
- When a feature branch is ready for testing and release, make a PR to `main` for deployment and testing on the TEST infrastructure as above.

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI. Modifying the `entity-api-spec.yaml` file and commititng the changes to github should update the API shown on SmartAPI. SmartAPI allows users to register API documents.  The documentation is associated with this github account: api-developers@hubmapconsortium.org.
