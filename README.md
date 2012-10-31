ms3
===

ms3 is a mock server intended to replace the dependency on AWS S3 when testing a project.

Current features are:
- create/delete/list buckets
- enable/disable versioning in buckets
- get/put objects (with versions)


For more up to date features, please look at tests/test_s3_operations.py.

1. Setting up
-------------
In order to get a virtualenv with all dependencies installed just run `./setup-development.sh`

2. Running
----------
In order to get a ms3 server up and running (for development purposes), run `python -m ms3.app`.

For running the test: `python tests/test_s3_operations.py`

You can find out more details regarding the configuration options by typing:
    python -m ms3.app --help


ms3 is released under MIT licence (see LICENSE file).
