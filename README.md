# DTN-as-a-Service v2 agent

# Deployment
Load configuration from CONF_FILE env variable to specify the following configuration. The format of the configuration follows the flask configuration format.

# Configuration
Most agent configuration is done in `config.py`. There is a sample config in `config.py.sample` for reference.
If a custom config file is created, make sure it is bind mounted in your container environment (if used).

# Note
You may need --privileged for SSD trim to work. 