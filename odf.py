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
		super(ODFHeader, self).__init__(padding=padding)
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
			l_values.append(Decimal(ls_values[1]))	# ODF_OPEN
			l_values.append(Decimal(ls_values[2]))	# ODF_HIGH
			l_values.append(Decimal(ls_values[3]))	# ODF_LOW
			l_values.append(Decimal(ls_values[4]))	# ODF_CLOSE
			l_values.append(Decimal(ls_values[5]))	# ODF_VOLUME

		except:
			raise

		return l_values

	def to_dict(self, s_odf_basename):
		""" Return dictionary representation used for DB writes.
		@param s_odf_basename: Base name (w/o suffix) of the ODF
		"""
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
				'padding' : 32,
				'storloc' : 1,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'TRADING_START_RECNO' : {
				'ld_fields' : [
					{'TRADING_START_RECNO_STORLOC' : 'H'},
					{'TRADING_START_RECNO' : 'd'},
				],
				'padding' : 32,
				'storloc' : 2,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'TRADING_RECS_PERDAY': {
				'ld_fields' : [
					{'TRADING_RECS_PERDAY_STORLOC' : 'H'},
					{'TRADING_RECS_PERDAY': 'd'},
				],
				'padding' : 32,
				'storloc' : 3,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'IDF_CURRENCY' : {
				'ld_fields' : [
					{'IDF_CURRENCY_STORLOC' : 'H'},
					{'IDF_CURRENCY' : 'd',},
				],
				'padding' : 32,
				'storloc' : 4,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'IDF_CURRENCY_MAX_DECIMALS' : {
				'ld_fields' : [
					{'IDF_CURRENCT_MAX_DECIMALS_STORLOC' : 'H'},
					{'IDF_CURRENCY_MAX_DECIMALS' : 'd'},
				],
				'padding' : 32,
				'storloc' : 5,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			}
		},
		{
			'SPLIT_FACTOR' : {
				'ld_fields' : [
					{'SPLIT_FACTOR_STORLOC' : 'H'},
					{'SPLIT_FACTOR' : 'd'},
				],
				'padding' : 32,
				'storloc' : 6, 
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'CURRENCY_VALUE_OF_POINT' : {
				'ld_fields' : [
					{'CURRENCY_VALUE_OF_POINT_STORLOC' : 'H'},
					{'CURRENCY_VALUE_OF_POINT' : 'd'},
				],
				'padding' : 32,
				'storloc' : 7,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		{
			'TICK' : {
				'ld_fields' : [
					{'TICK_STORLOC' : 'H'},
					{'TICK' : 'd'},
				],
				'padding' : 32,
				'storloc' : 8,
				'b_text' : False,
				'fn_parse' : dc.Decimal, 
			},
		},
		{
			'OHLC_DIVIDER' : {
				'ld_fields' : [
					{'OHLC_DIVIDER_STORLOC' : 'H'},
					{'OHLC_DIVIDER' : 'd'},
				],
				'padding' : 32,
				'storloc' : 9,
				'b_text' : False,
				'fn_parse' : dc.Decimal, 
			},
		},
		{
			'LAST_FCED_RECNO' : {
				'ld_fields' : [
					{'LAST_FCED_RECNO_STORLOC' : 'H'},
					{'LAST_FCED_RECNO' : 'd'},
				],
				'padding' : 32,
				'storloc' : 10,
				'b_text' : False,
				'fn_parse' : dc.Decimal, 
			},
		},
		{
			'HIGHEST_RECNO' : {
				'ld_fields' : [
					{'HIGHEST_RECNO_STORLOC' : 'H'},
					{'HIGHEST_RECNO' : 'd'},
				],
				'padding' : 32,
				'storloc' : 11,
				'b_text' : False,
				'fn_parse' : dc.Decimal, 
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
				'fn_parse' : dc.Decimal, 
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
				'fn_parse' : dc.Decimal,
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
		self.d_recno_index = {}

		# Each binary ODF record is 42 bytes long.
		self.record_size = 42

	def validate_headers(self):
		recno_count = 1
		
		for i, odf_header in enumerate(self.l_odf_headers):
			if not (i == recno_count-1):
				raise ODFException("Failed Header Integrity Test.")
			recno = odf_header.get_recno()
			if not (recno == recno_count):
				raise ODFException("Failed Header Integrity Test.")
			recno_count += 1

		if self.b_fill_missing_headers:
			if not (recno_count == 14):
				raise ODFException("Failed Header Integrity Test.")
		else:
			if not (recno_count == 8):
				raise ODFException("Failed Header Integrity Test.")

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
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for odf_header in self.l_odf_headers:
			odf_header.read_bin_stream(fp_bin_odf)
			# Check if record size is correct (42 bytes)
			if not (odf_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")
			recno = odf_header.get_recno()
			self.d_recno_index[recno] = odf_header
		self.validate_headers()

		# Parse body
		d_dedup_dict = {}
		try:
			while True:
				odf_body = ODFBody()
				odf_body.read_bin_stream(fp_bin_odf)
				# Check if record size is correct (42 bytes)
				if not (odf_body.get_size() == self.record_size):
					raise ODFException("Failed Header Integrity Test.")
				if odf_body.get_recno() == 0:
					continue
				self.dedup(d_dedup_dict, odf_body)
				recno = odf_body.get_recno()
				self.d_recno_index[recno] = odf_body
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
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for odf_header in self.l_odf_headers:
			if odf_header.is_in_text():
				odf_header.read_text_stream(fp_txt_odf)
				recno = odf_header.get_recno()
				self.d_recno_index[recno] = odf_header

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
				recno = odf_body.get_recno()
				self.d_recno_index[recno] = odf_body
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
		last_header_recno = 0
		# First hash all headers into the dup-detect dict
		for odf_header in self.l_odf_headers:
			if buf is None:
				buf = odf_header.to_bin()
			else:
				buf = buf + odf_header.to_bin()
			last_header_recno = odf_header.get_recno()
			#self.dup_detect_bin(d_dup_dict, odf_header)

		# First hash all records into the dup-detect dict
		first_recno = last_header_recno + 1
		last_recno = self.l_odf_body[-1].get_recno()
		odf_index = 0
		
		null_odf_body = ODFBody()

		# Store each record at byte location ((ODF_RECNO-1) * 42)+1
		# Store missing records as null entries.
		for recno in range(first_recno, last_recno+1):
			current_valid_recno = self.l_odf_body[odf_index].get_recno()

			if recno == current_valid_recno:
				buf = buf + self.l_odf_body[odf_index].to_bin()
				odf_index += 1
			else:
				buf = buf + null_odf_body.to_bin()

		return buf

	def to_bin_file(self, s_odf_bin):
		fp_odf_bin = open(s_odf_bin, "wb")
		buf = self.to_bin()
		fp_odf_bin.write(buf)
		fp_odf_bin.close()

	def to_dict(self, s_odf_basename):
		""" Create a list of dicts to write to DD. Does deduplication along the fly.
		"""
		ld_odf_recs = []

		for odf_header in self.l_odf_headers:
			ld_odf_recs.append(odf_header.to_dict(s_odf_basename))

		for odf_body in self.l_odf_body:
			ld_odf_recs.append(odf_body.to_dict(s_odf_basename))

		return ld_odf_recs

	def __repr__(self):
		""" Return a text representation of the ODF.
		"""
		buf = ""

		for odf_header in self.l_odf_headers:
			buf = buf + str(odf_header)

		for odf_body in self.l_odf_body:
			buf = buf + str(odf_body)

		return buf

	def get_highest_recno(self):
		return self.l_odf_body[-1].get_recno()


	def recno_exists(self, recno):
		if recno in self.d_recno_index:
			return True
		return False

	def get_fifo_arr(self, 
					fifo_count, 
					trading_start_recno):
		""" Return a FIFO array from ODF contents
		"""
		h = self.get_highest_recno() + 1
		c = 1

		l_fifo_arr = []
		while True:
			h = h - 1
			if h < trading_start_recno:
				return l_fifo_arr
			if not self.recno_exists(h):
				continue
			
			odf_h = self.d_recno_index[h]

			odf_h_open = odf_h.get_field("ODF_OPEN")
			odf_h_high = odf_h.get_field("ODF_HIGH")
			odf_h_low = odf_h.get_field("ODF_LOW")
			odf_h_close = odf_h.get_field("ODF_CLOSE")
			
			if odf_h_open == odf_h_high and odf_h_high == odf_h_low and \
				odf_h_low == odf_h_close:
				continue

			l_fifo_arr.append({
					'FIFO_RECNO' : c,
					'FIFO_OPEN' : odf_h_open,
					'FIFO_HIGH' : odf_h_high,
					'FIFO_LOW' : odf_h_low,
					'FIFO_CLOSE' : odf_h_close,
				})

			c +=1

			if c > fifo_count:
				return l_fifo_arr

		return l_fifo_arr

	def get_header_value(self, header_storloc):
		
		if header_storloc not in d_recno_index:
			return dc.Decimal('0')

		odf_header_rec = self.d_recno_index[header_storloc]
		
		return odf_header_rec.get_header_value()

	def get_field(recno, s_field_name):

		odf_record = self.d_recno_index[recno]

		return odf_record.get_field(s_field_name)

	def add_missing_record(recno, dc_open, dc_high, dc_low, dc_close, dc_volume=dc.Decimal('0')):

		odf_record = ODFBody(d_fields={
				'ODF_RECNO' : recno,
				'ODF_OPEN' : dc_open,
				'OPEN_HIGH' : dc_high,
				'OPEN_LOW' : dc_low,
				'OPEN_CLOSE' : dc_close,
				'OPEN_VOLUME' : dc_volume,
			})

		self.d_recno_index[recno] = odf_record
		self.l_odf_body.append(odf_record)

	def add_missing_header(header_storloc, dc_header_value):
		d_header_layout = self.ld_header_layout[header_storloc]
		d_header_kwargs = list(d_header_layout.values())[0]
		s_header_name = list(d_header_layout.keys())[0]
		s_header_storloc = "_".join(s_header_name, "STORLOC")
		d_fields = {
					s_header_storloc : header_storloc,
					s_header_name : dc_header_value,
					}

		odf_hdr_rec = ODFHeader(d_fields=d_fields, **d_header_kwargs)

		self.l_odf_headers.append(odf_hdr_rec)
		self.d_recno_index[header_storloc] = odf_hdr_rec


	""" These are actually underlying storage-mechanism dependent, and shoul;d BE
		made proxy methods to call storage-mechanism object methods
	"""

	def is_recno_out_of_limits(self, recno, trading_start_recno, trading_recs_perday):
		n = recno

		while n > 0:
			if n > trading_start_recno and n < (trading_recs_perday + trading_start_recno):
				return False
			n -= 1400

		return True

	def find_highest_recno(self, trading_start_recno, trading_recs_perday):
		# highest record no. in the odf
		r = self.l_odf_body[-1].get_recno()

		while self.is_recno_out_of_limits(r, trading_start_recno, trading_recs_perday):
			r -= 1

		return r
		
	def find_first_non_zero_open(self, trading_start_recno):
		r = trading_start_recno
		dc_odf_open = dc.Decimal('0')
		while True:
			if r not in d_recno_index:
				return dc.Decimal('0')
			odf_rec = self.d_recno_index[r]

			dc_odf_open = odf_rec.get_field('ODF_OPEN')

			if dc_odf_open > 0:
				break
			r += 1
		return dc_odf_open

	def find_prev_highest_recno_close(self, prev_fce_obj):
		
		highest_recno_close = 0

		if  prev_fce_obj is not None:
			ohlc_divider = prev_fce_obj.get_ohlc_divider()
			highest_recno_close = prev_fce_obj.get_highest_recno_close() / ohlc_divider

		return highest_recno_close


		

def open_odf_bin(s_odf_bin):
	odf_obj = ODF()
	fp_odf_bin = open(s_odf_bin, "rb")
	odf_obj.read_bin_stream(fp_odf_bin)
	fp_odf_bin.close()
	return odf_obj

def open_odf_text(s_odf_txt):
	odf_obj = ODF()
	fp_odf_txt = open(s_odf_txt, "r")
	odf_obj.read_txt_stream(fp_odf_txt)
	fp_odf_txt.close()
	return odf_obj

def open_odf_dd(s_exchange, s_odf_basename):
	odf_obj = None
	return odf_obj	
