
# Create logger
import logging
log = logging.getLogger(__name__)

class ODFException(Exception):
	""" Generic Exception class for ODF utilities
	"""
	pass

class ODFInvalidPadding(ODFException):
	""" Invalid padding value used when creating a binary structure
	"""
	pass

class ODFUnimplimented(ODFException):
	""" Attempt to call an abstract (unimplimented) method.
	"""
	pass

class ODFInvalidField(ODFException):
	""" Attempt to dereference an invalid key in an internal dictionary.
	"""
	pass

class ODFIOError(ODFException):
	""" Error during I/O. String arg should mention specific details.
	"""
	pass

class ODFEOF(ODFException):
	""" End-of-stream detected when reading a file or other stream.
	"""
	pass

class ODFDBException(ODFException):
	pass
