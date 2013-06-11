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

# Required by ODF utilities
import struct as st
import decimal as dc
from odfexcept import *
from binary import BinaryStruct

# Create logger
import logging
log = logging.getLogger(__name__)

class FIFOHeaderEncoder(BinaryStruct):
	""" ODF Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, storloc, b_text, fn_parse):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.storloc = storloc
		self.b_text = b_text
		self.fn_parse = fn_parse
		super(FIFOHeaderEncoder, self).__init__(padding)
		s_storloc_field = self.get_field_names()[0]
		self.d_fields[s_storloc_field] = self.storloc


	def is_in_text(self):
		""" Does this header field appear in the source text file?
		"""
		return self.b_text

	def parse_txt_buf(self, s_buf):
		""" Does not impliment parsing from a text buffer.
		"""
		raise ODFUnimplimented()

	def parse_txt_stream(self, fp_txt):
		""" Parse the given text stream, and return a list of values
		@param fp_txt: Text stream.
		"""
		l_values = [0 for d_field in self.ld_fields]

		l_values[0] = self.storloc

		line = fp_txt.readline()
		if line == "" or not line:
			raise ODFIOError("Empty file")
		l_values[1] = self.fn_parse(line.strip())

		#log.debug(l_values)
		return l_values

	def get_header_name(self):
		return list(self.ld_fields[1].keys())[0]

	def get_header_value(self):
		return self.d_fields[self.get_header_name()]

	def get_storloc(self):
		return self.storloc

	def get_recno(self):
		return self.get_storloc()

	def __repr__(self):
		""" Return a text representation of this object
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = d_field.keys()[0]
			ls_values.append(str(self.d_fields[s_field]))
		# Return only the main field value, and not padding or 'storloc'
		s_buf = str(ls_values[1]) + '\n'
		return s_buf

	def to_dict(self, s_odf_basename):

		s_header_name = self.get_header_name()
		f_header_val = self.get_header_value()
		odf_recno = self.get_storloc()
		'''
		d_odf_hdr_rec = {
			'ODF_NAME': s_odf_basename,
			'ODF_RECNO': odf_recno,
			s_header_name : dc.Decimal(str(f_header_val)),
		}
		'''
		d_odf_hdr_rec = {
			'ODF_NAME': s_odf_basename,
			'FIFO_RECNO': odf_recno,
			'FIFO_OPEN': dc.Decimal(str(f_header_val)),
			'FIFO_HIGH' : dc.Decimal(str(0)),
			'FIFO_LOW' : dc.Decimal(str(0)),
			'FIFO_CLOSE' : dc.Decimal(str(0)),
		}

		return d_odf_hdr_rec

class FIFORecordEncoder(BinaryStruct):
	""" Represents an individual record in a FIFO file.
	"""

	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'FIFO_RECNO' : 'H'},
		{'FIFO_OPEN' : 'd'},
		{'FIFO_HIGH' : 'd'},
		{'FIFO_LOW' : 'd'},
		{'FIFO_CLOSE' : 'd'},
	]

	def __init__(self):
		""" Constructor
		"""
		super(FIFORecordEncoder, self).__init__()

	def get_recno(self):
		return self.get_field("FIFO_RECNO")

	def parse_txt_buf(self, s_buf):
		raise ODFUnimplimented()

	def parse_txt_stream(self, fp_txt):
		""" Read & parse values from a text stream, return as list of values.
		@param fp_txt: Text Stream.
		"""
		l_values = []
		try:

			line = fp_txt.readline()

			if line == "" or not line:
				raise ODFEOF("Empty file")

			ls_values = line.strip().split(',')

			l_values.append(int(ls_values[0]))		# FIFO_RECNO
			l_values.append(float(ls_values[1]))	# FIFO_OPEN
			l_values.append(float(ls_values[2]))	# FIFO_HIGH
			l_values.append(float(ls_values[3]))	# FIFO_LOW
			l_values.append(float(ls_values[4]))	# FIFO_CLOSE

		except:
			raise

		return l_values

	def to_dict(self, s_odf_basename):
		fifo_recno = self.get_field("FIFO_RECNO")
		f_fifo_open = self.get_field("FIFO_OPEN")
		f_fifo_high = self.get_field("FIFO_HIGH")
		f_fifo_low = self.get_field("FIFO_LOW")
		f_fifo_close = self.get_field("FIFO_CLOSE")

		d_fifo_rec = {
					'ODF_NAME': s_odf_basename,
					'FIFO_RECNO': fifo_recno,
					'FIFO_OPEN': dc.Decimal(str(f_fifo_open)),
					'FIFO_HIGH' : dc.Decimal(str(f_fifo_high)),
					'FIFO_LOW' : dc.Decimal(str(f_fifo_low)),
					'FIFO_CLOSE' : dc.Decimal(str(f_fifo_close)),
					}

		return d_fifo_rec

	def __repr__(self):
		""" Return text representation of this object (CSV's)
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = d_field.keys()[0]
			ls_values.append(str(self.d_fields[s_field]))
		s_buf = ','.join(ls_values) + '\n'
		return s_buf
