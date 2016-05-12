'''
S3RAP
'''
__title__ = 's3rap'
__version__ = '0.3'
__author__ = 'Bob Colner'
__license__ = 'MIT'
__copyright__ = 'Copyright 2016 Bob Colner'

from . import s3rap

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
