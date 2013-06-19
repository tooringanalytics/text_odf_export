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

import os
import os.path
import re

from binary import BinaryStruct
from odfexcept import *

import logging
log = logging.getLogger(__name__)

class FCEPathSpec(object):

	def __init__(self,
				s_app_dir,
				s_exchange_basename,
				s_symbol,
				s_odf_basename):
		super(FCEPathSpec, self).__init__()
		self.init_params(s_app_dir, s_exchange_basename, s_symbol, s_odf_basename)

	def init_params(self, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
		matchobj = re.match(r'^(.*\-18)(\d+).*$', s_odf_basename)

		s_jsunnoon = matchobj.group(2)

		s_fce_basename = matchobj.group(1)

		self.odf_jsunnoon = int(s_jsunnoon)

		self.prev_odf_jsunnoon = self.odf_jsunnoon - 7

		self.s_fce_header_file_name = ''.join([s_fce_basename, str(self.odf_jsunnoon), '01', '.fce'])

		self.s_prev_fce_header_file_name = ''.join([s_fce_basename, str(self.prev_odf_jsunnoon), '01', '.fce'])
		
		self.s_fce_s3_prefix = os.sep.join([s_exchange_basename, "fce", s_symbol])

		self.s_fce_tmp = os.sep.join([s_app_dir, 'tmp', s_exchange_basename, 'fce', s_symbol])
		if not os.path.exists(self.s_fce_tmp):
			os.makedirs(self.s_fce_tmp)

class FCEHeader(BinaryStruct):
	""" ODF Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, fn_parse):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.fn_parse = fn_parse
		super(FCEHeader, self).__init__(padding=padding)


	def parse_txt_buf(self, s_buf):
		""" Does not impliment parsing from a text buffer.
		"""
		raise ODFUnimplimented()

	def parse_txt_stream(self, fp_txt):
		""" Parse the given text stream, and return a list of values
		@param fp_txt: Text stream.
		"""
		raise ODFUnimplimented

	def get_header_name(self):
		return list(self.ld_fields[0].keys())[0]

	def get_header_value(self):
		return self.d_fields[self.get_header_name()]

	def __repr__(self):
		""" Return a text representation of this object
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = d_field.keys()[0]
			ls_values.append(str(self.d_fields[s_field]))
		# Return only the main field value, and not padding or 'storloc'
		s_buf = str(ls_values[0]) + '\n'
		return s_buf

	def to_dict(self, s_fce_basename):

		s_header_name = self.get_header_name()
		dc_header_val = self.get_header_value()

		d_fce_hdr_rec = {
			'FCE_NAME' : s_fce_basename,
			s_header_name : dc_header_val
		}

		return d_fce_hdr_rec

class FCE(BinaryStruct):

	ld_header_layout = [
		{
			'GMT_OFFSET' : {
				'ld_fields' : [
					# {'<field_name>' : 'x/c/...' },
					{'GMT_OFFSET' : 'l'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'TRADING_START_RECNO' : {
				'ld_fields' : [
					{'TRADING_START_RECNO' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'TRADING_RECS_PERDAY': {
				'ld_fields' : [
					{'TRADING_RECS_PERDAY': 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'IDF_CURRENCY' : {
				'ld_fields' : [
					{'IDF_CURRENCY' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'IDF_CURRENCY_MAX_DECIMALS' : {
				'ld_fields' : [
					{'IDF_CURRENCY_MAX_DECIMALS' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			}
		},
		{
			'SPLIT_FACTOR' : {
				'ld_fields' : [
					{'SPLIT_FACTOR' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'CURRENCY_VALUE_OF_POINT' : {
				'ld_fields' : [
					{'CURRENCY_VALUE_OF_POINT' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
		{
			'TICK' : {
				'ld_fields' : [
					{'TICK' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int, 
			},
		},
		{
			'OHLC_DIVIDER' : {
				'ld_fields' : [
					{'OHLC_DIVIDER' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int, 
			},
		},
		{
			'LAST_FCED_RECNO' : {
				'ld_fields' : [
					{'LAST_FCED_RECNO' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int, 
			},
		},
		{
			'HIGHEST_RECNO' : {
				'ld_fields' : [
					{'HIGHEST_RECNO' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int, 
			},
		},
		{
			'HIGHEST_RECNO_CLOSE' : {
				'ld_fields' : [
					{'HIGHEST_RECNO_CLOSE' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int, 
			},
		},
		{
			'PREV_HIGHEST_RECNO_CLOSE' : {
				'ld_fields' : [
					{'PREV_HIGHEST_RECNO_CLOSE' : 'L'},
				],
				'padding' : 6,
				'fn_parse' : int,
			},
		},
	]

	def __init__(self, config=None, *kargs, **kwargs):
		super(FCEHeader, self).__init__(*kargs, **kwargs)
		self.d_hdr_index = {}
		for d_header in self.ld_header_layout:
			s_hdr_name = list(d_header.keys())[0]
			fce_header = FCEHeader(**d_header[s_hdr_name])
			self.l_fce_headers.append(fce_header)
			self.d_hdr_index[s_hdr_name] = fce_header
		# Each binary ODF record is 42 bytes long.
		self.record_size = 10
		
		# Load FCE from config -- to create a new FCE Header object.
		if config is not None:
			for s_hdr_name, fce_header in self.d_hdr_index.items():
				s_config_attr_name = s_hdr_name.tolower()
				value = 0
				if hasattr(config, s_config_attr_name):
					value = getattr(config, s_config_attr_name)
				fce_header.set_field(s_hdr_name, value)


	def validate_headers(self):
		
		num_headers = len(self.l_fce_headers)
			
		if not num_headers == 13:
			raise ODFException("Invalid Number of headers")

		for fce_header in self.l_fce_header:
			if not fce_header.get_size() == self.record_size:
				raise ODFException("Invalid header size")

	def read_bin_stream(self, fp_bin_fce):
		""" Read and parse FCE from a binary stream. 
		@param fp_bin_fce: Binary stream.
		"""
		# Parse headers.
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for fce_header in self.l_fce_headers:
			fce_header.read_bin_stream(fp_bin_odf)
			# Check if record size is correct (42 bytes)
			if not (fce_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")
			s_hdr_name = fce_header.get_header_name()
			self.d_hdr_index[s_hdr_name] = fce_header
		self.validate_headers()

	def to_bin(self):
		""" Pack this ODF into its binary format.
		"""
		buf = b''
		# First hash all headers into the dup-detect dict
		for fce_header in self.l_fce_headers:
			buf = buf + fce_header.to_bin()
		return buf

	def to_bin_file(self, s_fce_bin):
		fp_fce_bin = open(s_fce_bin, "wb")
		buf = self.to_bin()
		fp_fce_bin.write(buf)
		fp_fce_bin.close()

	def to_dict(self, s_fce_basename):
		""" Create a list of dicts to write to DD. Does deduplication along the fly.
		"""
		ld_fce_recs = []

		for fce_header in self.l_fce_headers:
			ld_fce_recs.append(fce_header.to_dict(s_fce_basename))

		return ld_fce_recs

	def __repr__(self):
		""" Return a text representation of the FCE.
		"""
		buf = ""

		for fce_header in self.l_fce_headers:
			buf = buf + str(fce_header)

		return buf

	def get_ohlc_divider(self):
		return self.d_hdr_index['OHLC_DIVIDER'].get_header_value()

	def get_highest_recno_close(self):
		return self.d_hdr_index['HIGHEST_RECNO_CLOSE'].get_header_value()
