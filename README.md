# DTN-as-a-Service v2 agent

# Deployment
Load configuration from CONF_FILE env variable to specify the following configuration. The format of the configuration follows the flask configuration format.

# Configuration
app.config['FILE_LOC'] (default = '/data') : Location of files to be transferred.

# Note
You may need --privileged for SSD trim to work. 