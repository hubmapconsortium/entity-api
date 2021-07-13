#!/bin/bash

FILE="$1"
SCRIPTDIRECTORY=$(dirname ${BASH_SOURCE[0]})
if (( $# == 1)); then
  if test -f "$FILE"; then
    ${SCRIPTDIRECTORY}/general_schema_template_transformer.py $FILE > ${SCRIPTDIRECTORY}/my-spec.yaml
    var=$( cat ${SCRIPTDIRECTORY}/my-spec.yaml)
  else
    echo "$FILE does not exist"
  fi
else
  echo "$# arguments were passed. Please pass exactly 1 argument that is the name of an existing file"
fi
if test -f "./my-spec.yaml"; then
  if test -z "$var"; then
    rm $SCRIPTDIRECTORY/my-spec.yaml
  fi
fi
