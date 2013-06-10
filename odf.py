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

class ODFHeader(BinaryStruct):
	""" ODF Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, storloc, b_text, fn_parse):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.storloc = storloc
		self.b_text = b_text
		self.fn_parse = fn_parse
		super(ODFHeader, self).__init__(padding)
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
			'ODF_RECNO': odf_recno,
			'ODF_OPEN': dc.Decimal(str(f_header_val)),
			'ODF_HIGH' : dc.Decimal(str(0)),
			'ODF_LOW' : dc.Decimal(str(0)),
			'ODF_CLOSE' : dc.Decimal(str(0)),
			'ODF_VOLUME' : dc.Decimal(str(0)),
		}

		return d_odf_hdr_rec

class ODFBody(BinaryStruct):
	""" Represents an individual record in an ODF file.
	"""

	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'ODF_RECNO' : 'H'},
		{'ODF_OPEN' : 'd'},
		{'ODF_HIGH' : 'd'},
		{'ODF_LOW' : 'd'},
		{'ODF_CLOSE' : 'd'},
		{'ODF_VOLUME' : 'd'},
	]

	def __init__(self):
		""" Constructor
		"""
		super(ODFBody, self).__init__()

	def get_recno(self):
		return self.get_field("ODF_RECNO")

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

			l_values.append(int(ls_values[0]))		# ODF_RECNO
			l_values.append(float(ls_values[1]))	# ODF_OPEN
			l_values.append(float(ls_values[2]))	# ODF_HIGH
			l_values.append(float(ls_values[3]))	# ODF_LOW
			l_values.append(float(ls_values[4]))	# ODF_CLOSE
			l_values.append(float(ls_values[5]))	# ODF_VOLUME

		except:
			raise

		return l_values

	def to_dict(self, s_odf_basename):
		odf_recno = self.get_field("ODF_RECNO")
		f_odf_open = self.get_field("ODF_OPEN")
		f_odf_high = self.get_field("ODF_HIGH")
		f_odf_low = self.get_field("ODF_LOW")
		f_odf_close = self.get_field("ODF_CLOSE")
		f_odf_volume = self.get_field("ODF_VOLUME")

		d_odf_rec = {
					'ODF_NAME': s_odf_basename,
					'ODF_RECNO': odf_recno,
					'ODF_OPEN': dc.Decimal(str(f_odf_open)),
					'ODF_HIGH' : dc.Decimal(str(f_odf_high)),
					'ODF_LOW' : dc.Decimal(str(f_odf_low)),
					'ODF_CLOSE' : dc.Decimal(str(f_odf_close)),
					'ODF_VOLUME' : dc.Decimal(str(f_odf_volume)),
					}

		return d_odf_rec

	def __repr__(self):
		""" Return text representation of this object (CSV's)
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = d_field.keys()[0]
			ls_values.append(str(self.d_fields[s_field]))
		s_buf = ','.join(ls_values) + '\n'
		return s_buf


class ODF(BinaryStruct):
	""" ODF File converter, and in-memory representation.
	"""

	""" The header layout specifies the locations and contents
	of each individual header record in the ODF. The contents
	of each entry provide arguments to the ODFHeader Constructor
	for each header record.
	"""
	ld_header_layout = [
		{
			'GMT_OFFSET' : {
				'ld_fields' : [
					# {'<field_name>' : 'x/c/...' },
					{'GMT_OFFSET_STORLOC' : 'H'},
					{'GMT_OFFSET' : 'd'},
				],
				'padding' : 28,
				'storloc' : 1,
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'TRADING_START_RECNO' : {
				'ld_fields' : [
					{'TRADING_START_RECNO_STORLOC' : 'H'},
					{'TRADING_START_RECNO' : 'd'},
				],
				'padding' : 28,
				'storloc' : 2,
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'TRADING_RECS_PERDAY': {
				'ld_fields' : [
					{'TRADING_RECS_PERDAY_STORLOC' : 'H'},
					{'TRADING_RECS_PERDAY': 'd'},
				],
				'padding' : 28,
				'storloc' : 3,
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'IDF_CURRENCY' : {
				'ld_fields' : [
					{'IDF_CURRENCY_STORLOC' : 'H'},
					{'IDF_CURRENCY' : 'd',},
				],
				'padding' : 28,
				'storloc' : 4,
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'IDF_CURRENCY_MAX_DECIMALS' : {
				'ld_fields' : [
					{'IDF_CURRENCT_MAX_DECIMALS_STORLOC' : 'H'},
					{'IDF_CURRENCY_MAX_DECIMALS' : 'd'},
				],
				'padding' : 28,
				'storloc' : 5,
				'b_text' : True, 
				'fn_parse' : float,
			}
		},
		{
			'SPLIT_FACTOR' : {
				'ld_fields' : [
					{'SPLIT_FACTOR_STORLOC' : 'H'},
					{'SPLIT_FACTOR' : 'd'},
				],
				'padding' : 28,
				'storloc' : 6, 
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'CURRENCY_VALUE_OF_POINT' : {
				'ld_fields' : [
					{'CURRENCY_VALUE_OF_POINT_STORLOC' : 'H'},
					{'CURRENCY_VALUE_OF_POINT' : 'd'},
				],
				'padding' : 28,
				'storloc' : 7,
				'b_text' : True, 
				'fn_parse' : float,
			},
		},
		{
			'TICK' : {
				'ld_fields' : [
					{'TICK_STORLOC' : 'H'},
					{'TICK' : 'L'},
				],
				'padding' : 36,
				'storloc' : 8,
				'b_text' : False,
				'fn_parse' : int, 
			},
		},
		{
			'OHLC_DIVIDER' : {
				'ld_fields' : [
					{'OHLC_DIVIDER_STORLOC' : 'H'},
					{'OHLC_DIVIDER' : 'L'},
				],
				'padding' : 36,
				'storloc' : 9,
				'b_text' : False,
				'fn_parse' : int, 
			},
		},
		{
			'LAST_FCED_RECNO' : {
				'ld_fields' : [
					{'LAST_FCED_RECNO_STORLOC' : 'H'},
					{'LAST_FCED_RECNO' : 'L'},
				],
				'padding' : 36,
				'storloc' : 10,
				'b_text' : False,
				'fn_parse' : int, 
			},
		},
		{
			'HIGHEST_RECNO' : {
				'ld_fields' : [
					{'HIGHEST_RECNO_STORLOC' : 'H'},
					{'HIGHEST_RECNO' : 'L'},
				],
				'padding' : 36,
				'storloc' : 11,
				'b_text' : False,
				'fn_parse' : int, 
			},
		},
		{
			'HIGHEST_RECNO_CLOSE' : {
				'ld_fields' : [
					{'HIGHEST_RECNO_CLOSE_STORLOC' : 'H'},
					{'HIGHEST_RECNO_CLOSE' : 'd'},
				],
				'padding' : 32,
				'storloc' : 12,
				'b_text' : False,
				'fn_parse' : float, 
			},
		},
		{
			'PREV_HIGHEST_RECNO_CLOSE' : {
				'ld_fields' : [
					{'PREV_HIGHEST_RECNO_CLOSE_STORLOC' : 'H'},
					{'PREV_HIGHEST_RECNO_CLOSE' : 'd'},
				],
				'padding' : 32,
				'storloc' : 13,
				'b_text' : False, 
				'fn_parse' : float,
			},
		}
	]

	def __init__(self, b_fill_missing_headers=False):
		""" Constructor
		"""
		self.l_odf_headers = []
		self.b_fill_missing_headers = b_fill_missing_headers
		for d_header in self.ld_header_layout:
			s_hdr_name = list(d_header.keys())[0]
			# Only add a header field if it is a 'compulsory' field
			# or if we have to fill missing header fields.
			if d_header[s_hdr_name]['b_text'] or b_fill_missing_headers:
				odf_header = ODFHeader(**d_header[s_hdr_name])
				self.l_odf_headers.append(odf_header)

		self.l_odf_body = []

		# Each binary ODF record is 42 bytes long.
		self.record_size = 42

	def validate_headers(self):
		recno_count = 1
		
		for i, odf_header in enumerate(self.l_odf_headers):
			assert(i == recno_count-1)
			recno = odf_header.get_recno()
			assert(recno == recno_count)
			recno_count += 1

		if self.b_fill_missing_headers:
			assert(recno_count == 14)
		else:
			assert(recno_count == 8)

	def dedup(self, d_dedup_dict, odf_obj):
		recno = odf_obj.get_recno()

		if recno in d_dedup_dict:
			log.debug("Duplicate detected with recno: %d" % recno)
			pass

		d_dedup_dict[recno] = odf_obj

	def get_dedup_keys(self, d_dedup_dict):
		return sorted(d_dedup_dict.keys())

	def get_dedup_objs(self, d_dedup_dict):
		l_keys = self.get_dedup_keys(d_dedup_dict)

		l_dedup_objs = []
		for key in l_keys:
			l_dedup_objs.append(d_dedup_dict[key])

		return l_dedup_objs

	def read_bin_stream(self, fp_bin_odf):
		""" Read and parse ODF from a binary stream. Also eliminate duplicate records.
		For duplicates, the most recently seen record overrides previous ones.
		@param fp_bin_odf: Binary stream.
		"""
		# Parse headers.
		for odf_header in self.l_odf_headers:
			odf_header.read_bin_stream(fp_bin_odf)
			# Check if record size is correct (42 bytes)
			assert(odf_header.get_size() == self.record_size)

		self.validate_headers()

		# Parse body
		#l_odf_body = []
		d_dedup_dict = {}
		try:
			while True:
				odf_body = ODFBody()
				odf_body.read_bin_stream(fp_bin_odf)
				# Check if record size is correct (42 bytes)
				assert(odf_body.get_size() == self.record_size)
				#l_odf_body.append(odf_body)
				self.dedup(d_dedup_dict, odf_body)
				#log.debug(odf_body.get_field('ODF_RECNO'))
		except ODFEOF as err:
			# EOF breaks the loop.
			pass
		except:
			raise
		self.l_odf_body = self.get_dedup_objs(d_dedup_dict)	

	def read_text_stream(self, fp_txt_odf):
		""" Read and parse ODF from text stream. Also eliminate duplicate records.
		For duplicates, the most recently seen record overrides previous ones.
		@param fp_txt_odf: Text stream
		"""

		# Parse headers
		for odf_header in self.l_odf_headers:
			if odf_header.is_in_text():
				odf_header.read_text_stream(fp_txt_odf)
		
		self.validate_headers()

		# Parse body
		#l_odf_body = []
		d_dedup_dict = {}
		try:
			while True:
				odf_body = ODFBody()
				odf_body.read_text_stream(fp_txt_odf)
				#l_odf_body.append(odf_body)
				self.dedup(d_dedup_dict, odf_body)
		except ODFEOF as err:
			pass
		except:
			raise

		self.l_odf_body = self.get_dedup_objs(d_dedup_dict)	


	def to_bin(self):
		""" Pack this ODF into its binary format.
		"""

		l_odf_objs = []
		buf = None
		
		# First hash all headers into the dup-detect dict
		for odf_header in self.l_odf_headers:
			if buf is None:
				buf = odf_header.to_bin()
			else:
				buf = buf + odf_header.to_bin()
			#self.dup_detect_bin(d_dup_dict, odf_header)

		# First hash all records into the dup-detect dict
		for odf_body in self.l_odf_body:
			if buf is None:
				buf = odf_body.to_bin()
			else:
				buf = buf + odf_body.to_bin()
			
			#self.dup_detect_bin(d_dup_dict, odf_body)

		# we have to sort recnos in asc. order from the hash, before we encode
		#l_keys = sorted(d_dup_dict.keys())

		# now pack all records in order of increasing recno.
		#for key in l_keys:
		#	buf = buf + d_dup_dict[key].to_bin()

		return buf

	def dup_detect(self, d_dict, s_odf_basename, odf_obj):
		s_key = self.dup_key(s_odf_basename, odf_obj)
		if d_dict.has_key(s_key):
			log.debug("Duplicate record: " + s_key)
			#assert(False)
			pass

		d_dict[s_key] = odf_obj.to_dict(s_odf_basename)

	def dup_key(self, s_odf_basename, odf_obj):
		return ''.join([s_odf_basename, '_', str(odf_obj.get_recno())])

	def to_dict(self, s_odf_basename):
		""" Create a list of dicts to write to DD. Does deduplication along the fly.
		"""
		ld_odf_recs = []
		
		#d_dup_dict = {}

		for odf_header in self.l_odf_headers:
			#self.dup_detect(d_dup_dict, s_odf_basename, odf_header)
			ld_odf_recs.append(odf_header.to_dict(s_odf_basename))

		for odf_body in self.l_odf_body:
			#self.dup_detect(d_dup_dict, s_odf_basename, odf_body)
			ld_odf_recs.append(odf_body.to_dict(s_odf_basename))

		return ld_odf_recs #d_dup_dict.values()

	def __repr__(self):
		""" Return a text representation of the ODF.
		"""
		buf = ""

		for odf_header in self.l_odf_headers:
			buf = buf + str(odf_header)

		for odf_body in self.l_odf_body:
			buf = buf + str(odf_body)

		return buf

