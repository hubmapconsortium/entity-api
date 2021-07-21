# Schema Templating

A collection of files and scripts used to modifying Openapi specification files in yaml format.

## How to run

Run build-schema.sh in this directory with 1 argument: a file path to a yaml schema "template" file.
The directory example-yaml-templates has a number of yaml files that have tags of the pattern "X-replace-..."
When build-schema.sh is run with the path to the desired file, general_schema_template_transformer.py will be executed
The X-replace tags have under them url's or direct file paths to the location of yaml files where desired information is located.
If that yaml file also has X-replace tags, this process will repeat until all replacements are made. The final result
is output as a new yaml file called my-spec.yaml. If build-schema.sh does not receive precisely 1 argument, an error is thrown.
If the file given as an argument does not exist, an error is also thrown.

### Usage:
````angular2html
./build-schema.sh example-yaml-templates/search-api-spec-TEMPLATE.yaml
````

## A note on schema_template_transformer.py

This python script is deprecated. It had similar functionality to general_schema_template_transformer.py however it only could
replace portions of a single file by retrieving enumerated lists from an external yaml file by url. It is kept in this directory
only for context and record keeping purposes. 


