# entity-api for HuBMAP
A set of web service calls to return information about HuBMAP entities.

The webservices are accessible through the /entities restful endpoint.
A description of the API calls is found here: [Entities API](https://drive.google.com/open?id=16C5vINOV53mKO5hIpFycbSdETsi6SIYd2FzB4Py2jBI).  (You may need to ask for permission to view this document)

## Deployment Steps
This application includes an **app.properties.example** file in the /conf directory.  Copy the file and rename it **app.properties**.  Modify **app.properties** with the appropriate information.

This code currently runs on port **5006**.


### Deployment to Development
This application has a dependency on the [HuBMAP Commons Code](https://github.com/hubmapconsortium/commons).  During development use the `requirements_dev.txt` file to manange the dependencies.  This file links to the HuBMAP Commons code on the local filesystem, not the one checked into github.  If you seem to be having problems during development, you may also want to uninstall and re-install this package through pip:

`python3 -m pip uninstall hubmap-commons`

`python3 -m pip install -r requirements_dev.txt`

### Deployment to Test/Production
This application has a dependency on the [HuBMAP Commons Code](https://github.com/hubmapconsortium/commons).  To ensure you have the latest code, you may need to periodically run this command update the code:

`python3 -m pip install -r requirements.txt`
