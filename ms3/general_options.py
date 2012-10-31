""" Helper module for handling common options """
import tornado.options
from tornado.options import define, options


define("debug", default=True, type=bool,
       metavar="True|False", help="debug mode")
define("config", default="config/ms3.conf", type=str,
       metavar="CONFIG FILE", help="Alternative configuration file")
define("datadir", default="data", type=str, metavar="DATA DIR",
       help="The directory where the files should be stored")


def get_options():
    """ Alias for tornado.options.options """
    return tornado.options.options


def parse_options(args=None):
    """ Helper for parsing the options in a consistent fashion """
    tornado.options.parse_command_line(args=args)
    try:
        tornado.options.parse_config_file(options.config)
    except IOError:
        pass
    tornado.options.parse_command_line(args=args)
