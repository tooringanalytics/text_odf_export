#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@author Anshuman P.Kanetkar

text_odf_export: Text file to ODF export tool.

Copyright (C) 2013, Anshuman P.Kanetkar

All rights reserved. 

* Licensed under terms specified in the LICENSE file distributed with this program.

DISCLAIMER:

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''

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
