import re
import os
import time
import shutil
import hashlib
import datetime
import lxml.etree

XMLNS = "http://doc.s3.amazonaws.com/2006-03-01"


def t(tag, text, **attrs):
    element = lxml.etree.Element(tag, **attrs)
    if not isinstance(text, basestring):
        text = str(text)
    element.text = text
    return element


def ea(element, *children):
    for child in children:
        element.append(child)


def e(tag, *children, **attrs):
    element = lxml.etree.Element(tag, **attrs)
    ea(element, *children)
    return element


def as_date(timestamp):
    result = datetime.datetime.fromtimestamp(timestamp)
    return result.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def httpdate(timestamp):
    """
http://stackoverflow.com/questions/225086/rfc-1123-date-representation-in-python

        Return a string representation of a date according to RFC 1123
        (HTTP/1.1). The supplied date must be in UTC.
    """
    dt = datetime.datetime.fromtimestamp(timestamp)
    weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
             "Oct", "Nov", "Dec"][dt.month - 1]
    return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (
        weekday, dt.day, month, dt.year, dt.hour, dt.minute, dt.second)


def xml_string(obj):
    return lxml.etree.tostring(obj, pretty_print=True)


class AWSObject(object):
    pass


def compare_entries(entryA, entryB):
    return cmp(entryA.version_id, entryB.version_id)


class Entry(AWSObject):

    @property
    def complete_path(self):
        return os.path.join(self.base_path, self.name)

    def __init__(self, name, base_path, versioned=False):
        self.versioned = False
        self.name = name
        self.key = name
        self.version_id = None
        if versioned:
            self.key = re.sub("\.\d+\.\d+$", "", self.key)
            self.version_id = float(re.search(".(\d+.\d+)$", name).group(1))
            self.is_latest = False
        self.base_path = base_path
        self.created_at = None
        if base_path:
            self._complete_metadata()

    def _complete_metadata(self):
        entry_path = os.path.join(self.base_path, self.name)
        stat = os.stat(entry_path)
        self.created_at = stat.st_ctime
        return stat

    def set_headers(self, handler):
        pass


class BucketEntry(Entry):

    @property
    def etag(self):
        with open(self.complete_path, "r") as fp:
            return hashlib.md5(fp.read()).hexdigest()

    def _complete_metadata(self):
        stat = super(BucketEntry, self)._complete_metadata()
        self.size = stat.st_size
        self.modified_at = stat.st_mtime
        return stat

    def read(self):
        with open(os.path.join(self.base_path, self.name), "rb") as fp:
            return fp.read()

    def xml(self, versions=False):
        result = None
        if not versions:
            result = e("Contents")
        else:
            if self.size > 0:
                tag = "Version"
            else:
                tag = "DeleteMarker"
            result = e(tag,
                       t("VersionId", "%.6f" % self.version_id),
                       t("IsLatest", self.is_latest))
        ea(result,
           t("Key", self.key),
           t("LastModified", as_date(self.modified_at)),
           t("ETag", self.etag),
           t("Size", self.size),
           t("StorageClass", "STANDARD"),
           Owner().xml())
        return result

    def set_headers(self, handler):
        handler.set_header('Last-Modified', httpdate(self.created_at))


def is_more_recent(entryA, entryB):
    if entryA is None:
        return True
    v1 = int(entryA.name.split(".")[-1])
    v2 = int(entryB.name.split(".")[-1])
    return v2 > v1


def make_entry_dir(entry_path):
    dirname = os.path.dirname(entry_path)
    try:
        os.makedirs(dirname)
    except OSError:
        pass


def remove_entry_dir(entry_path):
    try:
        os.unlink(entry_path)
    except OSError:
        pass
    dirname = os.path.dirname(entry_path)
    try:
        os.rmdir(dirname)
    except OSError:
        pass


class Bucket(Entry):

    METADATA = "metadata"
    METADATA_PROPS = ["versioned"]

    def __init__(self, name, base_path):
        super(Bucket, self).__init__(name, base_path)

    def _complete_metadata(self):
        stat = super(Bucket, self)._complete_metadata()
        metadata_path = os.path.join(self.complete_path, self.METADATA)
        self._parse_metadata(metadata_path)

    def _parse_metadata(self, path):
        if not os.path.exists(path):
            return
        with open(path, "r") as fp:
            for line in fp:
                args = line.split("=", 1)
                if len(args) < 2:
                    continue
                key, value = args
                if key in self.METADATA_PROPS:
                    setattr(self, key, eval(value))

    def enable_versioning(self):
        self.versioned = True
        self._write_metadata()

    def disable_versioning(self):
        self.versioned = False
        self._write_metadata()

    def _write_metadata(self):
        metadata_path = os.path.join(self.complete_path, self.METADATA)
        with open(metadata_path, "w") as fp:
            for key in self.METADATA_PROPS:
                fp.write("%s=%s\n" % (key, repr(getattr(self, key))))

    def delete(self):
        shutil.rmtree(self.complete_path, False)

    @classmethod
    def create(cls, name, datadir):
        try:
            os.makedirs(os.path.join(datadir, name))
        except (OSError, IOError) as exception:
            return None
        return Bucket(name, datadir)

    @classmethod
    def get_all_buckets(cls, base_path):
        results = []
        for entry in os.listdir(base_path):
            results.append(cls(entry, base_path=base_path))
        return results

    def get_entry(self, key, version_id=None):
        if self.versioned:
            if version_id:
                key = "%s.%s" % (key, version_id)
            else:
                versions = self.list_versions(prefix=key)
                if not versions:
                    return None
                return versions[0]
        try:
            return BucketEntry(key, self.complete_path,
                               versioned=self.versioned)
        except (IOError, OSError):
            return None

    def set_entry(self, key, value):
        if self.versioned:
            key = "%s.%.6f" % (key, time.time())
        entry_path = os.path.join(self.complete_path, key)
        make_entry_dir(entry_path)
        with open(entry_path, "w") as fp:
            fp.write(value)
        return BucketEntry(key, self.complete_path)

    def copy_entry(self, key, src_entry):
        # print "Copy at %.6f" % time.time(), "=>", key
        return self.set_entry(key, src_entry.read())

    def delete_entry(self, key, version_id=None):
        if self.versioned:
            if not version_id:
                key = "%s.%.6f" % (key, time.time())
                with open(os.path.join(self.complete_path, key), "w") as fp:
                    pass
                return  # add a 0 bytes file for deleted marker
            else:
                key = "%s.%s" % (key, version_id)

        entry_path = os.path.join(self.complete_path, key)
        remove_entry_dir(entry_path)

    def list(self, prefix=None):
        results = {}
        bucket_path = os.path.join(self.base_path, self.name)
        for root, dirs, files in os.walk(bucket_path):
            for f in files:
                f = os.path.join(root, f).replace(bucket_path + "/", "")
                if prefix and not f.startswith(prefix):
                    continue
                if f != self.METADATA:
                    entry = BucketEntry(f, bucket_path, self.versioned)
                    if is_more_recent(results.get(entry.key), entry):
                        results[entry.key] = entry

        return [e for e in results.values() if e.size > 0]

    def list_versions(self, prefix=None):
        results = {}
        bucket_path = os.path.join(self.base_path, self.name)
        for root, dirs, files in os.walk(bucket_path):
            for f in files:
                f = os.path.join(root, f).replace(bucket_path + "/", "")
                if prefix and not f.startswith(prefix):
                    continue
                if f != self.METADATA:
                    entry = BucketEntry(f, bucket_path, self.versioned)
                    if not results.get(entry.key):
                        results[entry.key] = []
                    results[entry.key].append(entry)
        final = []
        for key in sorted(results.keys()):
            final.extend(sorted(results[key], cmp=compare_entries,
                                reverse=True))
        return final

    def xml(self):
        return e("Bucket",
                 t("Name", self.name),
                 t("CreationDate", as_date(self.created_at)))


class Response(AWSObject):

    def xml(self):
        return e(self.tag, xmlns=XMLNS)


class Owner(AWSObject):
    def xml(self):
        return e("Owner",
                 t("ID", "super-owner-id"),
                 t("DisplayName", "S3 Owner"))


class ListAllMyBucketsResponse(Response):
    tag = "ListAllMyBucketsResult"

    def __init__(self, buckets):
        self.buckets = buckets

    def xml(self):
        result = super(ListAllMyBucketsResponse, self).xml()
        result.append(Owner().xml())
        for bucket in self.buckets:
            result.append(bucket.xml())
        return result


class ListBucketResponse(Response):

    tag = "ListBucketResult"

    def __init__(self, bucket, entries):
        self.bucket = bucket
        self.entries = entries

    def xml(self):
        result = super(ListBucketResponse, self).xml()
        ea(result,
           t("Name", self.bucket.name),
           t("IsTruncated", "false"))
        ea(result, *[entry.xml() for entry in self.entries])
        return result


class ListBucketVersionsResponse(Response):

    tag = "ListVersionsResult"

    def __init__(self, bucket, entries):
        self.bucket = bucket
        self.entries = entries

    def xml(self):
        result = super(ListBucketVersionsResponse, self).xml()
        ea(result,
           t("KeyMarker", ""),
           t("VersionIdMarker", ""))
        ea(result, *[entry.xml(versions=True) for entry in self.entries])
        return result


class VersioningConfigurationResponse(Response):

    tag = "VersioningConfiguration"

    def __init__(self, bucket):
        self.bucket = bucket

    def xml(self):
        result = super(VersioningConfigurationResponse, self).xml()
        if self.bucket.versioned:
            ea(result,
               t("Status", "Enabled"))
        return result

class CopyObjectResponse(Response):

    tag = "CopyObjectResult"

    def __init__(self, entry):
        self.entry = entry

    def xml(self):
        result = super(CopyObjectResponse, self).xml()
        ea(result,
           t("LastModified", httpdate(self.entry.modified_at)),
           t("ETag", '"%s"' % self.entry.etag))
        return result
