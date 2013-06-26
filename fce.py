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
import decimal as dc

from binary import BinaryStruct
from odfexcept import *
from collections import OrderedDict

import logging
log = logging.getLogger(__name__)


class FCEHeader(BinaryStruct):
	""" ODF Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, fn_parse, value=0):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.fn_parse = fn_parse
		super(FCEHeader, self).__init__(padding=padding)
		s_hdr_name = self.get_header_name()
		self.d_fields[s_hdr_name] = value

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
		return dc.Decimal(str(self.d_fields[self.get_header_name()]))

	def __repr__(self):
		""" Return a text representation of this object
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = d_field.keys()[0]
			ls_values.append(str(self.d_fields[s_field]))
		# Return only the main field value, and not padding or 'storloc'
		s_buf = ','.join(ls_values) + '\n'
		return s_buf

	def to_dict(self):

		s_header_name = self.get_header_name()
		dc_header_val = self.get_header_value()

		d_fce_hdr_rec = OrderedDict()

		d_fce_hdr_rec[s_header_name] = dc_header_val

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
		super(FCE, self).__init__(*kargs, **kwargs)
		self.d_hdr_index = {}
		# Each FCE header record is 10 bytes long.
		self.record_size = 10
		
		# Load FCE from config -- to create a new FCE Header object.
		if config is not None:
			# To do: Make this simpler by using simple assignment statements
			# instead of this convoluted (but correct) logic.
			for (hdr_no, d_hdr_lyt) in enumerate(self.ld_header_layout):
				s_hdr_name, d_hdr_param = list(d_hdr_lyt.items())[0]
				s_config_attr_name = s_hdr_name.lower()
				config_attr_value = 0
				if hasattr(config, s_config_attr_name):
					config_attr_value = getattr(config, s_config_attr_name)
					if s_config_attr_name == "highest_recno_close" or s_config_attr_name == "prev_highest_recno_close":
						config_attr_value = config_attr_value * config.ohlc_divider
				fce_header = FCEHeader(value=config_attr_value, **d_hdr_param)
				self.d_hdr_index[s_hdr_name] = fce_header.get_header_value()


	def read_bin_stream(self, fp_bin_fce, key=None):
		""" Read and parse FCE from a binary stream. 
		@param fp_bin_fce: Binary stream.
		"""
		# Parse headers.
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for i in range(len(self.ld_header_layout)):
			d_hdr_param = list(self.ld_header_layout[i].values())[0]
			fce_header = FCEHeader(**d_hdr_param)
			fce_header.read_bin_stream(fp_bin_fce, key)
			if not (fce_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")
			s_hdr_name = fce_header.get_header_name()
			self.d_hdr_index[s_hdr_name] = fce_header.get_header_value(s_hdr_name)
		
	def to_bin(self, key=None):
		""" Pack this FCE into its binary format.
		"""
		buf = b''

		for i in range(len(self.ld_header_layout)):
			d_hdr_param = list(self.ld_header_layout[i].values())[0]
			fce_header = FCEHeader(**d_hdr_param)
			s_hdr_name = fce_header.get_header_name()
			hdr_value = int(self.d_hdr_index[s_hdr_name])
			fce_header.set_field(s_hdr_name, hdr_value)
			#log.debug(type(hdr_value))
			buf = buf + fce_header.to_bin(key)

		return buf

	def to_bin_file(self, s_fce_bin, key=None):
		fp_fce_bin = open(s_fce_bin, "wb")
		buf = self.to_bin(key)
		fp_fce_bin.write(buf)
		fp_fce_bin.close()

	def to_csv(self):
		return str(self)
	
	def to_csv_file(self, s_fce_header_csv_file_path):
		fp_fce_csv = open(s_fce_header_csv_file_path, "w")
		buf = self.to_csv()
		fp_fce_csv.write(buf)
		fp_fce_csv.close()

	def __repr__(self):
		""" Return a text representation of the FCE.
		"""
		buf = ""

		for i in range(len(self.ld_header_layout)):
			s_hdr_name = list(self.ld_header_layout[i].keys())[0]
			buf = buf + str(self.d_hdr_index[s_hdr_name]) + '\n'

		return buf

	def get_ohlc_divider(self):
		return self.d_hdr_index['OHLC_DIVIDER']

	def get_highest_recno_close(self):
		return self.d_hdr_index['HIGHEST_RECNO_CLOSE']
