## Donor Bulk Registration

Donors can be bulk registered by choosing the Bulk Registration -> Donors menu pick in the [Ingest UI](https://ingest.hubmapconsortium.org)
 `insert screen shot showing menu`
 
 To register multiple donors at once you'll be asked to upload a tsv file with one row of data per donor to be registered.  The tsv file has the following columns:
 
 | Column<br>Name | Required | Neo4j/WS<br>attrib | Description | Validation Rules |
 |-------------|----------|----------|-------------|------------------|
 | lab_id | no | donor.lab_donor_id | An id used by the lab for this donor. This id can be used when searching for the donor in the Ingest UI | Can be blank or an alpha numeric string less than 1024 characters |
 | lab_name | yes | donor.label | A de-identified name used by the lab. This name can be usd when searching for the donor in the Ingest UI | Must be a valid alpha-numeric string greater that 1 and less than 1024 characters |
 | selection_protocol | yes |  donor.protocol_url | The doi or doi url to the Protocols IO protocol describing the criteria used when selecting this donor, e.g. 10.17504/protocols.io.bjuxknxn or https://dx.doi.org/10.17504/protocols.io.bjuxknxn | A string that matches either of the patterns<br> - `https://dx.doi.org/##.####/protocols.io.*` <br> - `##.####/protocols.io.*` <br> where # is a numeric character and * matches any characters |
 | description | no | donor.description | A description of this donor | The field can be empty or contain an alphanumeric string less than 10,000 characters |
 