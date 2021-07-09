#!/bin/bash

FILE="$1"
SCRIPTDIRECTORY=$(dirname ${BASH_SOURCE[0]})
${SCRIPTDIRECTORY}/general_schema_template_transformer.py $FILE > ${SCRIPTDIRECTORY}/my-spec.yaml
#cat ${SCRIPTDIRECTORY}/new-spec-api.yaml > ${SCRIPTDIRECTORY}/my-spec.yaml
#rm ${SCRIPTDIRECTORY}/new-spec-api.yaml