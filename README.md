# ms3

[![Build Status](https://secure.travis-ci.org/skoobe/ms3.png)](http://secure.travis-ci.org/skoobe/ms3)

ms3 is a mock server intended to replace the dependency on AWS S3 when testing a project.

Current features are:
- create/delete/list buckets
- enable/disable versioning in buckets
- get/put objects (with versions)


For more up to date features, please look at tests/test_s3_operations.py.

## 1. Setting up
-------------
In order to get a virtualenv with all dependencies installed just run `./setup-development.sh`

Setting up the connection (assuming a global level function that provides an S3 connection):
```python
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from tornado.options import options

def current_s3_connection():
    """ Return an AWS S3 connection """
    if getattr(current_s3_connection, '_connection', None) is None:
        if options.use_ms3:
            _connection = S3Connection('X', 'Y',
                is_secure=options.ms3_https,
                port=options.ms3_port, host=options.ms3_host,
                calling_format=OrdinaryCallingFormat())
        else:
            _connection = S3Connection(options.aws_access_key,
                                       options.aws_secret_key)
        current_s3_connection._connection = _connection
    return current_s3_connection._connection
```

Starting with a test case:
```python
from ms3.testing import MS3Server

class ExampleTestCase(TestCase):

    def setUp(self):
        self.datadir = helpers.tests_dir("s3")
        MS3Server.start(self.datadir, port=9011, with_exec=True)

    def tearDown(self):
        MS3Server.stop()

    def test_example():
        s3_conn = current_s3_connection()
        buckets = s3_conn.get_all_buckets()
        ...
```

## 2. Running
----------
In order to get a ms3 server up and running (for development purposes), run `python -m ms3.app`.

For running the test: `python tests/test_s3_operations.py`

You can find out more details regarding the configuration options by typing:
    python -m ms3.app --help


ms3 is released under MIT licence (see LICENSE file).
