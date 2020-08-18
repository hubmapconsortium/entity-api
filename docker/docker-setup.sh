#!/bin/bash

# Set the version environment variable for the docker build
# Version number is from the VERSION file
export ENTITY_API_VERSION=`cat VERSION`

echo "ENTITY_API_VERSION: $ENTITY_API_VERSION"

# Copy over the src folder
cp -r ../src entity-api/


