#!/bin/bash

# Set the version environment variable for the docker build
# Version number is from the VERSION file
# Also remove newlines and leading/trailing slashes if present in that VERSION file
export ENTITY_API_VERSION=$(tr -d "\n\r" < ../VERSION | xargs)

echo "ENTITY_API_VERSION: $ENTITY_API_VERSION"

# Copy over the src folder
cp -r ../src entity-api/


