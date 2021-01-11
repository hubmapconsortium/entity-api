# entity-api for HuBMAP

A set of web service calls to return information about HuBMAP entities.

A description of the API calls is found here: [Entities API](http://smart-api.info/ui/12af775769ba65a684476960f5f87e72).

## Entities schema yaml

The yaml file `src/resources/hubmap-entities.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.

## API endpoints and examples

### Get all entity types

````
GET https://entity-api.refactor.hubmapconsortium.org/entity-types
````

Generated HTTP request code example:

````
GET /entity-types HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
````

It doesn't require a globus token to make the request. And the response returns a list of normalized entity types: `Collection`, `Dataset`, `Donor`, `Sample`.

### Get an entity by id

````
GET https://entity-api.refactor.hubmapconsortium.org/entities/<id>
````

Note: The `<id>` can be either a HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity

Generated HTTP request code example:

````
GET /entities/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token. And the result JSON looks like:

````
{
"contains_human_genetic_sequences": false,
"created_by_user_displayname": "HuBMAP Process",
"created_by_user_email": "hubmap@hubmapconsortium.org",
"created_by_user_sub": "3e7bce63-129d-33d0-8f6c-834b34cd382e",
"created_timestamp": 1596070576277,
"data_access_level": "consortium",
"data_types": "[\"sc_atac_seq_snare\"]",
"doi_suffix_id": "865LWLC946",
"entity_type": "Dataset",
"group_name": "hubmap-ucsd-tmc",
"group_uuid": "03b3d854-ed44-11e8-8bce-0e368f3075e8",
"hubmap_id": "HBM865.LWLC.946",
"ingest_metadata": "{'dag_provenance_list': [{'hash': 'de1b3fc', 'origin': 'https://github.com/hubmapconsortium/ingest-pipeline.git'}, {'hash': 'de1b3fc', 'origin': 'https://github.com/hubmapconsortium/ingest-pipeline.git'}, {'name': 'create_snap_and_analyze.cwl', 'hash': '04d75d8', 'origin': 'https://github.com/hubmapconsortium/sc-atac-seq-pipeline.git'}, {'name': 'scatac-csv-to-arrow.cwl', 'hash': 'fb19103', 'origin': 'https://github.com/hubmapconsortium/portal-containers.git'}], 'files': [{'rel_path': 'cellMotif.csv', 'type': 'csv', 'size': 2034481, 'description': 'Motif enrichment, per-cell', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'frags.sort.bed.gz', 'type': 'unknown', 'size': 28, 'description': 'Fragment file from Sinto', 'edam_term': 'EDAM_1.24.format_3003'}, {'rel_path': 'snaptools.snap', 'type': 'unknown', 'size': 2535790623, 'description': 'SNAP file (HDF5 convention) from SnapTools', 'edam_term': 'EDAM_1.24.format_3590'}, {'rel_path': 'BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R1_fastqc.zip', 'type': 'unknown', 'size': 1242685, 'description': 'FastQC report for BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R1.fastq, zip version', 'edam_term': 'EDAM_1.24.format_2333'}, {'rel_path': 'filtered_cell_by_bin.mtx', 'type': 'unknown', 'size': 115615323, 'description': 'Cell by bin matrix, after cell filtering, in MTX format', 'edam_term': 'EDAM_1.24.format_3916'}, {'rel_path': 'barcodes.txt', 'type': 'unknown', 'size': 70050, 'description': 'Row labels (cell barcodes) of filtered_cell_by_bin.mtx', 'edam_term': 'EDAM_1.24.format_3475'}, {'rel_path': 'snaptools.snap.qc', 'type': 'unknown', 'size': 575, 'description': 'QC results from SnapTools', 'edam_term': 'EDAM_1.24.data_3671'}, {'rel_path': 'GenesRanges.csv', 'type': 'csv', 'size': 3161162, 'description': 'Gene ranges used for peak calling', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R3_fastqc.zip', 'type': 'unknown', 'size': 1246777, 'description': 'FastQC report for BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R3.fastq, zip version', 'edam_term': 'EDAM_1.24.format_2333'}, {'rel_path': 'peaks.combined.bed', 'type': 'unknown', 'size': 7425, 'description': 'Peaks called across entire data set', 'edam_term': 'EDAM_1.24.format_3003'}, {'rel_path': 'bins.txt', 'type': 'unknown', 'size': 11155518, 'description': 'Column labels (genome bins) of filtered_cell_by_bin.mtx', 'edam_term': 'EDAM_1.24.format_3475'}, {'rel_path': 'BarcodeQualityControlDistributionBefore.pdf', 'type': 'pdf', 'size': 6399, 'description': '', 'edam_term': 'EDAM_1.24.format_3508'}, {'rel_path': 'BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R3_fastqc.html', 'type': 'unknown', 'size': 735628, 'description': 'FastQC report for BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R3.fastq, HTML version', 'edam_term': 'EDAM_1.24.format_2331'}, {'rel_path': 'BarcodeQualityControlDistributionAfter.pdf', 'type': 'pdf', 'size': 6211, 'description': '', 'edam_term': 'EDAM_1.24.format_3508'}, {'rel_path': 'cell_by_gene.hdf5', 'type': 'hdf5', 'size': 1358881735, 'description': 'Cell by gene matrix (smoothed with MAGIC), in HDF5 format', 'edam_term': 'EDAM_1.24.format_3590'}, {'rel_path': 'rmsk.bam', 'type': 'unknown', 'size': 4223630034, 'description': 'Aligned reads', 'edam_term': 'EDAM_1.24.format_2572'}, {'rel_path': 'alignment_qc.json', 'type': 'json', 'size': 302, 'description': 'Alignment quality control metrics, in JSON format', 'edam_term': 'EDAM_1.24.format_3464'}, {'rel_path': 'BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R1_fastqc.html', 'type': 'unknown', 'size': 728346, 'description': 'FastQC report for BUKMAP_20190529E.BUKMAP_2019529_SNARE2-AC_SR2_P4_N504_S4_R1.fastq, HTML version', 'edam_term': 'EDAM_1.24.format_2331'}, {'rel_path': 'umap_coords_clusters.csv', 'type': 'csv', 'size': 182031, 'description': 'Per-cell UMAP coordinates and cluster assignments', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'Bins.csv', 'type': 'csv', 'size': 35858957, 'description': 'All genome bins', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'PromotorRatioLogPlot.pdf', 'type': 'pdf', 'size': 100591, 'description': 'Log-log plot of promoter ratios', 'edam_term': 'EDAM_1.24.format_3508'}, {'rel_path': 'chromvar_variability_scores.csv', 'type': 'csv', 'size': 23013, 'description': 'Per-cell variability scores from chromVAR', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'chromvar_deviation_scores.csv', 'type': 'csv', 'size': 2000123, 'description': 'Per-cell deviation scores from chromVAR', 'edam_term': 'EDAM_1.24.format_3752'}, {'rel_path': 'output/umap_coords_clusters.factors.json', 'type': 'json', 'size': 94243, 'description': \"JSON-formatted information about this scATAC-seq's clustering.\", 'edam_term': 'EDAM_1.24.format_3464'}, {'rel_path': 'output/umap_coords_clusters.cell-sets.json', 'type': 'json', 'size': 96816, 'description': \"JSON-formatted information about the heirarchy scRNA-seq's cells.\", 'edam_term': 'EDAM_1.24.format_3464'}, {'rel_path': 'output/umap_coords_clusters.arrow', 'type': 'arrow', 'size': 148178, 'description': 'Input data relevant for visualization saved in columnar Apache Arrow format.', 'edam_term': 'EDAM_1.24.format_2333'}, {'rel_path': 'output/umap_coords_clusters.cells.json', 'type': 'json', 'size': 365140, 'description': 'JSON-formatted information about this scATAC-seq run including scatterplot coordinates and clustering.', 'edam_term': 'EDAM_1.24.format_3464'}, {'rel_path': 'output/umap_coords_clusters.csv', 'type': 'csv', 'size': 180236, 'description': 'Per-cell UMAP coordinates and cluster assignments', 'edam_term': 'EDAM_1.24.format_3752'}]}",
"last_modified_timestamp": 1598119021652,
"last_modified_user_displayname": "Bill Shirey",
"last_modified_user_email": "shirey@pitt.edu",
"last_modified_user_sub": "e19adbbb-73c3-43a7-b05e-0eead04f5ff8",
"local_directory_rel_path": "/University of California San Diego TMC/fb6757b606ac35be7fa85062fde9c2e1",
"pipeline_message": "the process ran",
"published_timestamp": 1598119021652,
"published_user_displayname": "Bill Shirey",
"published_user_email": "shirey@pitt.edu",
"published_user_sub": "e19adbbb-73c3-43a7-b05e-0eead04f5ff8",
"source_uuids":[
"28481bdc81b2fac9c645ec95fc0e1824"
],
"status": "QA",
"uuid": "fb6757b606ac35be7fa85062fde9c2e1"
}
````

Result filtering is supported via query string. For example:

````
GET https://entity-api.refactor.hubmapconsortium.org/entities/<id>?property=data_access_level
````

This returns the `data_access_level` property value directly instead of all the properties of this entity object.

### Get all entities of a given entity type 

````
GET https://entity-api.refactor.hubmapconsortium.org/<entity_type>/entities
````

Generated HTTP request code example:

````
GET /donor/entities HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token.

Result filtering is supported via query string:

````
GET https://entity-api.refactor.hubmapconsortium.org/<entity_type>/entities?property=uuid
````

This returns a list of UUIDs of the resulting entities instead of all the properties of each entity.

### Create an entity of the target entity type

````
POST https://entity-api.refactor.hubmapconsortium.org/entities/<entity_type>
````

Generated HTTP request code example:

````
POST /entities/donor HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
Content-Type: application/json
````

As you can see, it requires a globus token.

The same JSON envelop will be used in the request body: 

Create a new entity:

````
{
    all the standard properties defined in schema yaml for the target entity type...
}
````

### Update the properties of a given entity (except for Collection)

````
PUT https://entity-api.refactor.hubmapconsortium.org/entities/<id>
````

Generated HTTP request code example:

````
PUT /entities/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
Content-Type: application/json
````

As you can see, it requires a globus token.

The JSON request body will need to contain the properties (only the ones to be updated) defiend in the schema yaml file.

### Get all the ancestors of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/ancestors/<id>
````

Note: The `<id>` can be either a HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity

Generated HTTP request code example:

````
GET /ancestors/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token.

Result filtering is supported via query string:

````
GET https://entity-api.refactor.hubmapconsortium.org/ancestors/<id>?property=uuid
````

This returns a list of UUIDs of the resulting entities instead of all the properties of each entity.

### Get all the descendants of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/descendants/<id>
````

Generated HTTP request code example:

````
GET /descendants/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token.

Result filtering is supported via query string:

````
GET https://entity-api.refactor.hubmapconsortium.org/descendants/<id>?property=uuid
````

This returns a list of UUIDs of the resulting entities instead of all the properties of each entity.

### Get all the parents of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/parents/<id>
````

Generated HTTP request code example:

````
GET /parents/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token.

Result filtering is supported via query string:

````
GET https://entity-api.refactor.hubmapconsortium.org/parents/<id>?property=uuid
````

This returns a list of UUIDs of the resulting entities instead of all the properties of each entity.

### Get all the children of a given entity

````
GET https://entity-api.refactor.hubmapconsortium.org/children/<id>
````

Generated HTTP request code example:

````
GET /children/fb6757b606ac35be7fa85062fde9c2e1 HTTP/1.1
Host: entity-api.refactor.hubmapconsortium.org
Authorization: Bearer AgB3dyx2XqOxQNdxWXJP6vzeNpjnqNawnoaE3kqWwpVgVYjJ9jhyCBpmeBXdvrY2DG7ElBve5yvqpwcke1pEKiogVO
````

As you can see, it requires a globus token.

Result filtering is supported via query string:

````
GET https://entity-api.refactor.hubmapconsortium.org/children/<id>?property=uuid
````

This returns a list of UUIDs of the resulting entities instead of all the properties of each entity.

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
