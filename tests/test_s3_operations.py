import os
import shutil
import os.path
import helpers
import unittest2

from ms3.testing import MS3Server

from itertools import izip
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
from boto.exception import S3ResponseError, S3CreateError


def cleanup(dirname):
    shutil.rmtree(dirname, True)


def get_data_dir(dirname):
    data = os.path.join(os.path.dirname(os.path.abspath(__file__)), dirname)
    cleanup(data)
    os.makedirs(data)
    return data


def create_bucket_dir(datadir, dirname):
    os.makedirs(os.path.join(datadir, dirname))


class BucketOperationsTestCase(unittest2.TestCase):

    def setUp(self):
        self.datadir = get_data_dir('buckets')
        MS3Server.start(datadir=self.datadir)
        self.s3 = S3Connection('X', 'Y', is_secure=False,
                               host='localhost', port=9010,
                               calling_format=OrdinaryCallingFormat())

    def tearDown(self):
        self.s3.close()
        MS3Server.stop()
        cleanup(self.datadir)

    def test_empty_buckets_list(self):
        self.assertEquals([], self.s3.get_all_buckets())

    def test_one_bucket_list(self):
        create_bucket_dir(self.datadir, "bucket-A")
        results = self.s3.get_all_buckets()
        self.assertEquals(1, len(results))
        self.assertEquals("bucket-A", results[0].name)

    def test_two_buckets_list(self):
        create_bucket_dir(self.datadir, "bucket-A")
        create_bucket_dir(self.datadir, "bucket-B")
        results = self.s3.get_all_buckets()
        self.assertEquals(2, len(results))
        self.assertEquals(set(["bucket-A", "bucket-B"]),
                          set([b.name for b in results]))

    def test_enable_versioning(self):
        create_bucket_dir(self.datadir, "bucket-A")
        bucket = self.s3.get_bucket("bucket-A")
        self.assertTrue(bucket is not None)
        bucket.configure_versioning(True)
        result = bucket.get_versioning_status()
        self.assertEquals("Enabled", result['Versioning'])

    def test_disable_versioning(self):
        create_bucket_dir(self.datadir, "bucket-A")
        bucket = self.s3.get_bucket("bucket-A")
        self.assertTrue(bucket is not None)
        bucket.configure_versioning(True)
        result = bucket.get_versioning_status()
        self.assertEquals("Enabled", result['Versioning'])
        bucket.configure_versioning(False)
        result = bucket.get_versioning_status()
        self.assertEquals({}, result)

    def test_put_get_list_object(self):
        create_bucket_dir(self.datadir, "my-bucket")
        bucket = self.s3.get_bucket("my-bucket")
        self.assertTrue(bucket is not None)
        key = Key(bucket)
        key.name = "put-object"
        key.set_contents_from_string("Simple test")
        keys = bucket.get_all_keys()
        self.assertEquals(1, len(keys))
        self.assertEquals("put-object", keys[0].name)
        self.assertEquals("Simple test", keys[0].get_contents_as_string())

    @unittest2.skip("Takes too long to run")
    def test_put_large_file(self):
        create_bucket_dir(self.datadir, "my-bucket")
        chunks = 1024 * 16
        chunk = 64 * 1024
        with open("large-file", "w+") as fp:
            for _ in xrange(chunks):
                fp.write('a' * chunk)
        bucket = self.s3.get_bucket("my-bucket")
        key = Key(bucket)
        key.name = "large-object"
        key.set_contents_from_filename("large-file")
        keys = bucket.get_all_keys(prefix="large-object")
        self.assertEquals(1, len(keys))
        self.assertEquals(chunks * chunk, keys[0].size)
        os.unlink("large-file")

    def test_get_unknown_bucket(self):
        self.assertRaises(S3ResponseError, self.s3.get_bucket, "test-bucket")

    def test_no_save_for_0_bytes_objects(self):
        create_bucket_dir(self.datadir, "my-bucket")
        bucket = self.s3.get_bucket("my-bucket")
        key = Key(bucket)
        key.name = "zero-object"
        key.set_contents_from_string("")
        keys = bucket.get_all_keys(prefix="zero")
        self.assertEquals(0, len(keys))

    def test_create_bucket(self):
        bucket = self.s3.create_bucket("simple")
        keys = bucket.get_all_keys()
        self.assertEquals(0, len(keys))
        key = Key(bucket)
        key.name = "data"
        key.set_contents_from_string("simple data")
        created_bucket = self.s3.get_bucket("simple")
        self.assertTrue(created_bucket is not None)
        keys = created_bucket.get_all_keys()
        self.assertEquals(1, len(keys))
        self.assertEquals("simple data", keys[0].get_contents_as_string())

    def test_create_bucket_twice(self):
        self.s3.create_bucket("simple")
        self.assertRaises(S3CreateError, self.s3.create_bucket, "simple")

    def test_delete_bucket(self):
        bucket = self.s3.create_bucket("simple")
        key = Key(bucket)
        key.name = "data"
        key.set_contents_from_string("simple data")
        bucket.delete()
        self.assertRaises(S3ResponseError, self.s3.get_bucket, "simple")

    def test_delete_bucket_twice(self):
        bucket = self.s3.create_bucket("simple")
        key = Key(bucket)
        key.name = "data"
        key.set_contents_from_string("simple data")
        bucket.delete()
        self.assertRaises(S3ResponseError, bucket.delete)

    def test_copy_key_no_versioning(self):
        source = self.s3.create_bucket("source")
        destination = self.s3.create_bucket("destination")
        key = Key(source)
        key.name = "an/object"
        key.set_contents_from_string("This is an object")
        destination.copy_key("another/object", "source", "an/object")
        dest_keys = destination.get_all_keys()
        src_keys = source.get_all_keys()
        self.assertEquals(len(src_keys), len(dest_keys))
        self.assertEquals(src_keys[0].size, dest_keys[0].size)
        self.assertEquals(src_keys[0].etag, dest_keys[0].etag)

    def test_copy_key_src_versioning(self):
        source = self.s3.create_bucket("source")
        source.configure_versioning(True)
        destination = self.s3.create_bucket("destination")
        key = Key(source)
        key.name = "an/object"
        key.set_contents_from_string("This is an object")
        key.set_contents_from_string("This is a better version of an object")
        destination.copy_key("another/object", "source", "an/object")
        dest_keys = destination.get_all_keys()
        src_keys = source.get_all_keys()
        self.assertEquals(len(src_keys), len(dest_keys))
        self.assertEquals(src_keys[0].size, dest_keys[0].size)
        self.assertEquals(src_keys[0].etag, dest_keys[0].etag)
        self.assertEquals("This is a better version of an object",
                          dest_keys[0].get_contents_as_string())

    def test_copy_key_src_versioning_specific_version(self):
        source = self.s3.create_bucket("source")
        source.configure_versioning(True)
        destination = self.s3.create_bucket("destination")
        key = Key(source)
        key.name = "an/object"
        key.set_contents_from_string("This is an object")
        key.set_contents_from_string("This is a better version of an object")
        key.set_contents_from_string("Even better version")
        versions = source.get_all_versions(prefix="an/object")
        destination.copy_key("another/object", "source", "an/object",
                             src_version_id=versions[1].version_id)
        dest_keys = destination.get_all_keys()
        src_keys = source.get_all_keys()
        self.assertEquals(len(src_keys), len(dest_keys))
        self.assertNotEquals(src_keys[0].size, dest_keys[0].size)
        self.assertNotEquals(src_keys[0].etag, dest_keys[0].etag)
        self.assertEquals("This is a better version of an object",
                          dest_keys[0].get_contents_as_string())
        self.assertEquals("Even better version",
                          src_keys[0].get_contents_as_string())

    def test_copy_key_both_versioned(self):
        source = self.s3.create_bucket("source")
        source.configure_versioning(True)
        destination = self.s3.create_bucket("destination")
        destination.configure_versioning(True)
        key = Key(source)
        key.name = "an/object"
        key.set_contents_from_string("This is an object")
        key.set_contents_from_string("This is a better version of an object")
        key.set_contents_from_string("Even better version")
        versions = source.get_all_versions(prefix="an/object")
        for version in versions[::-1]:
            destination.copy_key("another/object", "source", "an/object",
                                 src_version_id=version.version_id)
        dest_keys = destination.get_all_keys()
        src_keys = source.get_all_keys()
        self.assertEquals(len(src_keys), len(dest_keys))
        dst_versions = destination.get_all_versions(prefix="another/object")
        self.assertEquals(len(versions), len(dst_versions))
        for s_version, d_version in izip(versions, dst_versions):
            self.assertEquals(s_version.size, d_version.size)
            self.assertEquals(s_version.etag, d_version.etag)


if __name__ == "__main__":
    helpers.run()
