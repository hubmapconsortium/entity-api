## Sample Donor Registration

(Tissue) Samples can be bulk registered by choosing the Bulk Registration -> Samples menu pick in the [Ingest UI](https://ingest.hubmapconsortium.org)
 `insert screen shot showing menu`
 
 To register multiple samples at once you'll be asked to upload a tsv file with one row of data per donor to be registered.  The tsv file has the following columns:
 
 | Column<br>Name | Required | entity-api<br>attrib | Description | Validation Rules |
 |-------------|----------|----------|-------------|------------------|
 | source_id | yes | sample.direct_ancestor_uuid | The id of the source/parent of the new sample, can be a UUID, HuBMAP ID or HuBMAP Sample ID per [this](https://portal.hubmapconsortium.org/docs/apis) | - This is required and should fit the format of a hubmap uuid, hubmap id, or hubmap submission id<br> - should be checked against the uuid-api for existance<br> - if sample_type == organ it must point to a donor<br> - if sample_type != organ it must point to a sample<br> -If rui_location is not blank cannot be the id of a donor |
 | lab_id | yes | sample.lab_tissue_sample_id  |An id used by the lab for this sample. This id can be used when searching for the donor in the Ingest UI | Must be an alpha numeric string less than 1024 characters |
 | sample_type | yes | specimen.specimen_type | The code specifying the type of sample | -Must be a code listed in the [tissue sample types file](https://github.com/hubmapconsortium/search-api/blob/main/src/search-schema/data/definitions/enums/tissue_sample_types.yaml) via case insensitive compare<br> -If rui_location is not blank cannot be 'organ' |
 | organ_type | maybe | specimen.organ | The code specifying the type of organ that the sample is | -if sample_type == organ must be a code from the [organ types file](https://github.com/hubmapconsortium/search-api/blob/main/src/search-schema/data/definitions/enums/organ_types.yaml) via case insensitive compare <br> -if sample_type != organ must be empty  |
 | sample_protocol | yes |  sample.protocol_url | The doi or doi url to the Protocols IO protocol describing how the sample was procured, e.g. 10.17504/protocols.io.bjuxknxn or https://dx.doi.org/10.17504/protocols.io.bjuxknxn | A string that matches either of the patterns<br> - `https://dx.doi.org/##.####/protocols.io.*` <br> - `##.####/protocols.io.*` <br> where # is a numeric character and * matches any characters |
 | description | no | sample.description | A description of this sample | The field can be empty or contain an alphanumeric string less than 10,000 characters |
 | rui_location | no | sample.rui_location | The json output from the RUI location registration interface.  Must not include any line breaks. | - Can be blan  <br> - If not blank must be a valid json string |