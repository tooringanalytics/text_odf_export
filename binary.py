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

import struct as st
from odfexcept import *
import decimal as dc

# Create logger
import logging
log = logging.getLogger(__name__)

from itertools import cycle

def xor_crypt(data, key):
	# For python 2, should use itertools.izip instead of zip
	# zip returns a list in python2, but returns iterator in py3.
	return b''.join([st.pack('B', x^y) for (x,y) in zip(data, cycle(key))])

class BinaryStruct(object):
	""" Base class for all packed binary structures. e.g. ODF Headers, data, FIFO headers etc.
	"""

	""" See http://docs.python.org/3/library/struct.html
	"""
	d_size = {
		'x' : 1,
		'c' : 1,
		'b' : 1,
		'B' : 1,
		'?' : 1,
		'h' : 2,
		'H' : 2,
		'l' : 4,
		'L' : 4,
		'q' : 8,
		'Q' : 8,
		'd' : 8,
	}

	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
	]

	""" Endianness has to be explicitly declared to disable auto-alignment.
	We currently use '<' i.e. little-endian encoding.
	"""
	s_endian = '<' # One of @,=,<,>,!

	""" Padding specifiers for 24, 28, 32, 36 bytes etc.
	"""
	_ld_append_6 = [
		{'pad1' : 'L'},
		{'pad2' : 'H'},
	]
	_ld_append_16 = [
		{'pad1' : 'd'},
		{'pad2' : 'd'},
	]

	_ld_append_24 = [
		{'pad1' : 'd'},
		{'pad2' : 'd'},
		{'pad3' : 'd'},
	]

	_ld_append_28 = [
		{'pad1' : 'd'},
		{'pad2' : 'd'},
		{'pad3' : 'd'},
		{'pad4' : 'L'},
	]

	_ld_append_32 = [
		{'pad1' : 'd'},
		{'pad2' : 'd'},
		{'pad3' : 'd'},
		{'pad4' : 'd'},
	]

	_ld_append_36 = [
		{'pad1' : 'd'},
		{'pad2' : 'd'},
		{'pad3' : 'd'},
		{'pad4' : 'd'},
		{'pad5' : 'L'},
	]

	def __init__(self, d_fields=None, padding=None):
		""" Constructor for BinaryStruct base class.
		@param padding: No. of bytes to append as padding. (default: None if zero)
		"""
		s_struct_fmt = self.s_endian
		self.d_fields = {}

		# Append the padding specifier to self.ld_fields
		if padding is not None:
			if padding == 6:
				self.ld_fields = self.ld_fields + self._ld_append_6
			elif padding == 16:
				self.ld_fields = self.ld_fields + self._ld_append_16
			elif padding == 24:
				self.ld_fields = self.ld_fields + self._ld_append_24
			elif padding == 28:
				self.ld_fields = self.ld_fields + self._ld_append_28
			elif padding == 32:
				self.ld_fields = self.ld_fields + self._ld_append_32
			elif padding == 36:
				self.ld_fields = self.ld_fields + self._ld_append_36
			else:
				raise ODFInvalidPadding()

		# Prepare the format string for binary decoing/encoding
		# and create a dict object with some initial values for all fields
		
		for d_field in self.ld_fields:
			s_width = list(d_field.values())[0]
			s_field_name = list(d_field.keys())[0]
			s_struct_fmt = s_struct_fmt + s_width
			self.d_fields[s_field_name] = 0

		if d_fields is not None:
			for k, v in d_fields.items():
				self.d_fields[k] = v

		# Compile the format string for packing/unpacking binary data
		self.st_struct = st.Struct(s_struct_fmt)
		self.s_struct_fmt = s_struct_fmt

	def __str__(self):
		""" Return the string representation of this type.
		"""
		return self.__repr__()

	def get_size(self):
		""" Calculate and return the size (in bytes) of this binary structure.
		"""
		size = 0
		for d_field in self.ld_fields:
			size += self.d_size[list(d_field.values())[0]]
		return size

	def get_field_names(self):
		""" Return a list of names of all binary fields in the order in which they appear.
		"""
		ls_fields = [ list(d_field.keys())[0] for d_field in self.ld_fields]
		return ls_fields

	def get_field(self, s_field):
		""" Return the value for the speicifed field.
		@param s_field: field name.
		"""
		if s_field in self.d_fields:
			return self.d_fields[s_field]
		raise ODFInvalidField("Unknown field %s" % s_field)
	
	def set_field(self, s_field, value):
		""" Set the value for the specifed field.
		@param s_field: field name.
		@param value: value to set.
		"""
		self.d_fields[s_field] = value

	def update_fields(self, d_fields):
		for k, v in d_fields.items():
			self.d_fields[k] = v


	""" 'Protected' methods. Not meant for public access, but have to be 
	implemented by derived classes if unimplimented.
	"""

	def parse_bin_buf(self, s_buf):
		""" Parse a binary buffer and return the unpacked values as a list.
		@param s_buf: Binary byte buffer, should be of size self.get_size()
		"""
		l_values = self.st_struct.unpack(s_buf)
		l_values = list(map(lambda x: dc.Decimal(str(x)) if type(x) is float else x, l_values))
		return l_values

	def parse_bin_stream(self, fp_bin, key=None):
		""" Read self.get_size() bytes from the given stream, and parse values.
		@param fp_bin: Binary byte stream
		"""
		read_size = self.get_size()
		#log.debug("read_size=%d" % read_size)
		s_buf = fp_bin.read(read_size)
		if s_buf is None or len(s_buf) == 0:
			#print "eof encountered"
			#log.debug(fp_bin.tell())
			raise ODFEOF()
		if key is not None:
			s_buf = xor_crypt(s_buf, key)
		return self.parse_bin_buf(s_buf)

	def parse_bin_stream_at(self, fp_bin, offset=0, key=None):
		""" Seek to a given offset in the byte stream, and parse values.
		@param fp_bin: Binary byte stream.
		@param offset: Relative offset to star read. (default: 0)
		"""
		fp_bin.fseek(offset)
		return self.parse_bin_stream(fp_bin, key)
	
	def parse_txt_buf(self, s_buf):
		""" Parse values froma  text buffer.
		@param s_buf: String buffer
		"""
		raise ODFUnimplimented()

	def parse_txt_stream(self, fp_txt):
		""" Read given text stream and return parsed values in a list.
		@param fp_txt: Text stream.
		"""
		raise ODFUnimplimented()
	
	def parse_txt_stream_at(self, fp_txt, offset=0):
		""" Seek to the given offset in the text stream, and parse values.
		@param fp_txt: Text stream.
		@param offset: Relative offset to start read. (default: 0)
		"""
		fp_txt.fseek(offset)
		return self.parse_txt_stream(fp_txt)

	def __repr__(self):
		""" Returns string representation of object. Must be implemented by derived class.
		"""
		raise ODFUnimplimented()

	""" 'Public' interface.
	"""

	def read_bin_stream(self, fp_bin, key=None):
		""" Read & parse a binary stream. Returns a dictionary object with field names & vals.
		@param fp_bin: Binary byte stream.
		"""
		l_values = self.parse_bin_stream(fp_bin, key)
		
		ls_fields = self.get_field_names()

		self.d_fields = dict(zip(ls_fields, l_values))
		
		#log.debug(self.d_fields)

		return self.d_fields

	def read_bin_stream_at(self, fp_bin, offset=0, key=None):
		""" Read & parse a binary stream starting from the given relative offset.
		@param fp_bin: Binary byte stream.
		@param offset: Relative offset (default: 0)
		"""
		l_values = self.parse_bin_stream_at(fp_bin, offset, key)
		
		ls_fields = self.get_field_names()

		self.d_fields = dict(zip(ls_fields, l_values))

		return self.d_fields

	def read_text_stream(self, fp_txt):
		""" Read & parse a text stream.
		@param fp_txt: Text Stream
		"""
		l_values = self.parse_txt_stream(fp_txt)
		
		ls_fields = self.get_field_names()

		self.d_fields = dict(zip(ls_fields, l_values))

		return self.d_fields

	def read_text_stream_at(self, fp_txt, offset=0):
		""" Read & parse a text stream starting from the given relative offset.
		@param fp_txt: Text stream.
		@param offset: Relative offset (default: 0)
		"""
		l_values = self.parse_txt_stream_at(fp_txt, offset)
		
		ls_fields = self.get_field_names()

		self.d_fields = dict(zip(ls_fields, l_values))

		return self.d_fields

	def to_bin(self, key=None):
		""" Pack all values into a binary buffer, and return it.
		"""
		l_values = list([])
		for d_field in self.ld_fields:
			#print "Key : " + d_field.keys()[0]
			#print "d_fields keys: " + ', '.join(self.d_fields.keys())
			s_field_name = list(d_field.keys())[0]
			l_values.append(self.d_fields[s_field_name])
		#print(l_values)
		#for val in l_values:
		#	print(type(val))
		#print(self.s_struct_fmt)
		#print(*l_values)
		buf = b''
		try:
			buf = st.pack(self.s_struct_fmt, *l_values)
		except:
			for value in l_values:
				log.debug(type(value))
			raise
		# XOR encrypt if a key is given
		if key is not None:
			buf = xor_crypt(buf, key)
		return buf

	def to_text(self):
		""" Return a text representation of this binary struct.
		"""
		return self.__repr__()