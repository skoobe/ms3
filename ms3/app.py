"""
    The Tornado application
"""
import os
import errno
import shutil
import socket
import hashlib
import logging
import urlparse
import tornado.web
import tornado.ioloop
import tornado.httpserver
from tornado.options import options, define

import ms3.general_options as general_options

from ms3.commands import (
    Bucket, ListAllMyBucketsResponse, xml_string,
    ListBucketResponse, ListBucketVersionsResponse,
    VersioningConfigurationResponse, CopyObjectResponse)

define("port", default=9009, type=int, metavar="PORT",
       help="Port on which we run this server (usually https port)")
define("internal_ssl", default=False, type=bool, metavar="True|False",
       help="Use internal SSL")
define("keyfile", default="certs/key.pem", type=str,
       help="Key File", metavar="PATH")
define("certfile", default="certs/cert.pem", type=str,
       help="Certificate File", metavar="PATH")
define("cafile", default="certs/ca.pem", type=str,
       help="CA Certificate File", metavar="PATH")


_logger = logging.getLogger(__name__)


class BaseHandler(tornado.web.RequestHandler):
    """ Common functionality for all handlers """
    @property
    def datadir(self):
        return self.application.datadir

    def echo(self):
        """ Debug function for a request """
        self.set_header('Content-Type', 'text/plain')
        request = self.request
        _logger.debug("Request headers")
        for key, value in request.headers.iteritems():
            _logger.debug("\t%s: %s", key, value)
        _logger.debug("Request arguments")
        for key, value in self.request.arguments.iteritems():
            _logger.debug("\t%s: %s" % (key, value))
        props = ["method", "uri", "body"]
        for key in props:
            _logger.debug("%s: %s", key.title(), getattr(request, key))

    def has_section(self, section):
        """
            Check if the request has as query argument the specified section
        """
        args = self.request.uri.split("?")
        if args and len(args) > 1 and section in args[1]:
            return True
        return False

    def has_header(self, header):
        """ Check if the request has a specified header """
        return header in self.request.headers

    def get_header(self, header):
        """ Get the value of the specified header """
        return self.request.headers[header]

    def get_bucket(self, name):
        """
            Helper for getting a bucket.
            Sends 404 back if the bucket is not found
        """
        try:
            return Bucket(name, self.datadir)
        except OSError as exception:
            _logger.warn(exception)
            self.send_error(404)

    def render_xml(self, result):
        """ Helper for rendering the response """
        self.write(xml_string(result.xml()))
        self.finish()


class CatchAllHandler(BaseHandler):
    """ Debug handler for inspecting requests """
    def get(self):
        self.echo()

    def post(self):
        self.echo()


class BucketHandler(BaseHandler):
    """ Handle for GET/PUT/DELETE operations on buckets """
    def get(self, name):
        bucket = self.get_bucket(name)
        if not bucket:
            return
        result = None
        prefix = self.get_argument("prefix", None)
        if self.has_section("versioning"):
            result = VersioningConfigurationResponse(bucket)
        elif self.has_section("versions"):
            result = ListBucketVersionsResponse(
                bucket, bucket.list_versions(prefix=prefix))
        else:
            result = ListBucketResponse(
                bucket, bucket.list(prefix=prefix))
        self.render_xml(result)

    def head(self, name):
        self.set_status(200)

    def put(self, name):
        if self.has_section("versioning"):
            bucket = self.get_bucket(name)
            if not bucket:
                return
            if '<Status>Enabled</Status>' in self.request.body:
                bucket.enable_versioning()
            else:
                bucket.disable_versioning()
        else:
            bucket = Bucket.create(name, self.datadir)
            if not bucket:
                _logger.warn("Could not create bucket %s", name)
                self.send_error(409)
                return
        self.echo()

    def delete(self, name):
        bucket = self.get_bucket(name)
        if not bucket:
            return
        bucket.delete()
        self.set_status(204)


class ListAllMyBucketsHandler(BaseHandler):
    """ Handler for listing all buckets """
    def get(self):
        result = ListAllMyBucketsResponse(Bucket.get_all_buckets(self.datadir))
        self.render_xml(result)

    def delete(self):
        shutil.rmtree(options.datadir, ignore_errors=True)
        try:
            os.makedirs(options.datadir)
        except (IOError, OSError):
            pass


class ObjectHandler(BaseHandler):
    """ Handle for GET/PUT/HEAD/DELETE on objects """
    def get(self, name, key):
        version_id = self.get_argument("versionId", None)
        bucket = self.get_bucket(name)
        if not bucket:
            return
        entry = bucket.get_entry(key, version_id=version_id)
        if not entry:
            self.send_error(404)
        else:
            entry.set_headers(self)
            self.write(entry.read())

    def put(self, name, key):
        bucket = self.get_bucket(name)
        if not bucket:
            return
        if not self.request.body:
            if self.has_header("x-amz-copy-source"):
                source_name, key_name = (self.get_header("x-amz-copy-source").
                                         split("/", 1))
                source = self.get_bucket(source_name)
                if not source:
                    return
                version_id = None
                if "?" in key_name:
                    key_name, args = key_name.split("?", 1)
                    args = urlparse.parse_qs(args)
                    if "versionId" in args:
                        version_id = args["versionId"][0]
                entry = source.get_entry(
                    key_name, version_id=version_id)
                if not entry or entry.size == 0:
                    _logger.warn("Could not find source entry or size is 0"
                                 " for %s/%s", source_name, key_name)
                    self.send_error(404)
                    return
                entry = bucket.copy_entry(key, entry)
                if entry:
                    response = CopyObjectResponse(entry)
                    self.render_xml(response)
            else:
                _logger.warn("Not accepting 0 bytes files")
                self.set_header('ETag', '"%s"' % hashlib.md5("").hexdigest())
        else:
            entry = bucket.set_entry(key, self.request.body)
            self.set_header('ETag', '"%s"' % entry.etag)

    def head(self, name, key):
        version_id = self.get_argument("versionId", None)
        bucket = self.get_bucket(name)
        if not bucket:
            return
        entry = bucket.get_entry(key, version_id=version_id)
        if not entry:
            self.send_error(404)
        else:
            self.set_header('ETag', '"%s"' % entry.etag)

    def delete(self, name, key):
        version_id = self.get_argument("versionId", None)
        bucket = self.get_bucket(name)
        if not bucket:
            return
        bucket.delete_entry(key, version_id=version_id)
        self.set_status(204)


def fix_TCPServer_handle_connection():
    """ Monkey-patching tornado to increase the maxium file size to 1.5 GB """
    import tornado.netutil
    from tornado.iostream import SSLIOStream, IOStream
    import ssl

    max_buffer_size = 1536 * 1024 * 1024  # 1.5GB
    read_chunk_size = 64 * 1024

    def _handle_connection(self, connection, address):
        if self.ssl_options is not None:
            assert ssl, "Python 2.6+ and OpenSSL required for SSL"
            try:
                connection = ssl.wrap_socket(connection,
                                             server_side=True,
                                             do_handshake_on_connect=False,
                                             **self.ssl_options)
            except ssl.SSLError, err:
                if err.args[0] == ssl.SSL_ERROR_EOF:
                    return connection.close()
                else:
                    raise
            except socket.error, err:
                if err.args[0] == errno.ECONNABORTED:
                    return connection.close()
                else:
                    raise
        try:
            if self.ssl_options is not None:
                stream = SSLIOStream(connection, io_loop=self.io_loop,
                                     max_buffer_size=max_buffer_size,
                                     read_chunk_size=read_chunk_size)
            else:
                stream = IOStream(connection, io_loop=self.io_loop,
                                  max_buffer_size=max_buffer_size,
                                  read_chunk_size=read_chunk_size)
            self.handle_stream(stream, address)
        except Exception:
            _logger.error("Error in connection callback", exc_info=True)

    tornado.netutil.TCPServer._handle_connection = _handle_connection


class MS3App(tornado.web.Application):
    """ """
    def __init__(self, args=None, debug=False):

        general_options.parse_options(args=args)

        handlers = [
            (r"/", ListAllMyBucketsHandler),
            (r"/([^/]+)/", BucketHandler),
            (r"/([^/]+)/(.+)", ObjectHandler),
            (r"/.*", CatchAllHandler)
        ]
        settings = {
            'debug': debug or options.debug
        }
        self.datadir = options.datadir
        if not os.path.isabs(self.datadir):
            self.datadir = os.path.normpath(os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "..",
                self.datadir))
        try:
            os.makedirs(self.datadir)
        except (OSError, IOError) as exception:
            _logger.warn("Tried to create %s: %s", self.datadir, exception)
        tornado.web.Application.__init__(self, handlers, **settings)
        fix_TCPServer_handle_connection()


def run(args=None):
    """ Helper for running the app """
    app = MS3App(args=args)

    ssl_options = None
    if options.internal_ssl:
        ssl_options = {
            'keyfile': options.keyfile,
            'certfile': options.certfile,
            'ca_certs': options.cafile
        }

    http_server = tornado.httpserver.HTTPServer(app, xheaders=True,
                                                ssl_options=ssl_options)
    http_server.listen(options.port)
    _logger.info("Using configuration file %s", options.config)
    _logger.info("Using data directory %s", app.datadir)
    _logger.info("Starting up on port %s", options.port)
    instance = tornado.ioloop.IOLoop().instance()
    instance.start()


if __name__ == "__main__":
    run()
