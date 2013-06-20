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
from collections import OrderedDict

# Create logger
import logging
log = logging.getLogger(__name__)

class ODFHeader(BinaryStruct):
	""" ODF Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, storloc, b_text, fn_parse, value=0):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.storloc = storloc
		self.b_text = b_text
		self.fn_parse = fn_parse
		super(ODFHeader, self).__init__(padding=padding)
		s_storloc_field = self.get_field_names()[0]
		s_header_field = self.get_field_names()[1]
		self.d_fields[s_storloc_field] = self.storloc
		self.d_fields[s_header_field] = value

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
		#print(line)
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

	def to_dict(self):

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
		d_odf_hdr_rec = OrderedDict()

		d_odf_hdr_rec['ODF_RECNO'] = odf_recno
		d_odf_hdr_rec['ODF_OPEN'] = dc.Decimal(str(f_header_val))
		d_odf_hdr_rec['ODF_HIGH'] = dc.Decimal(str(0))
		d_odf_hdr_rec['ODF_LOW'] = dc.Decimal(str(0))
		d_odf_hdr_rec['ODF_CLOSE'] = dc.Decimal(str(0))
		d_odf_hdr_rec['ODF_VOLUME'] = dc.Decimal(str(0))
		
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

	def __init__(self, d_fields=None):
		""" Constructor
		"""
		super(ODFBody, self).__init__(d_fields=d_fields)

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
			l_values.append(dc.Decimal(ls_values[1]))	# ODF_OPEN
			l_values.append(dc.Decimal(ls_values[2]))	# ODF_HIGH
			l_values.append(dc.Decimal(ls_values[3]))	# ODF_LOW
			l_values.append(dc.Decimal(ls_values[4]))	# ODF_CLOSE
			l_values.append(dc.Decimal(ls_values[5]))	# ODF_VOLUME

		except:
			raise

		return l_values

	def to_dict(self):
		""" Return dictionary representation used for DB writes.
		@param s_odf_basename: Base name (w/o suffix) of the ODF
		"""
		odf_recno = self.get_field("ODF_RECNO")
		f_odf_open = self.get_field("ODF_OPEN")
		f_odf_high = self.get_field("ODF_HIGH")
		f_odf_low = self.get_field("ODF_LOW")
		f_odf_close = self.get_field("ODF_CLOSE")
		f_odf_volume = self.get_field("ODF_VOLUME")

		d_odf_rec = OrderedDict()

		d_odf_rec['ODF_RECNO'] = odf_recno
		d_odf_rec['ODF_OPEN'] = dc.Decimal(str(f_odf_open))
		d_odf_rec['ODF_HIGH'] = dc.Decimal(str(f_odf_high))
		d_odf_rec['ODF_LOW'] = dc.Decimal(str(f_odf_low))
		d_odf_rec['ODF_CLOSE'] = dc.Decimal(str(f_odf_close))
		d_odf_rec['ODF_VOLUME'] = dc.Decimal(str(f_odf_volume))

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

	d_odf_dd_schema = {
		's_hash_key_name' : "ODF_NAME",
		'hash_key_proto_value' : str,
		's_range_key_name' : "ODF_RECNO",
		'range_key_proto_value' : int,
	}

	def __init__(self, b_fill_missing_headers=False):
		""" Constructor
		"""
		#self.l_odf_headers = []
		self.b_fill_missing_headers = b_fill_missing_headers
		'''
		for d_header in self.ld_header_layout:
			s_hdr_name = list(d_header.keys())[0]
			# Only add a header field if it is a 'compulsory' field
			# or if we have to fill missing header fields.
			if d_header[s_hdr_name]['b_text'] or b_fill_missing_headers:
				odf_header = ODFHeader(**d_header[s_hdr_name])
				self.l_odf_headers.append(odf_header)
		'''
		#self.l_odf_body = []
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

	def set_store(self, store):
		self.store = store


	def get_field(self, recno, s_field_name):
		odf_rec = self.d_recno_index[recno]
		return odf_rec[s_field_name]

	def read_bin_stream(self, fp_bin_odf):
		""" Read and parse ODF from a binary stream. Also eliminate duplicate records.
		For duplicates, the most recently seen record overrides previous ones.
		@param fp_bin_odf: Binary stream.
		"""
		# Parse headers.
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects

		for i in range(len(self.ld_header_layout)):
			d_header = self.ld_header_layout[i]
			s_hdr_name = list(d_header.keys())[0]
			d_hdr_param = list(d_header.values())[0]

			#if not d_hdr_param['b_text'] and not self.b_fill_missing_headers:
			#	continue

			odf_header = ODFHeader(**d_hdr_param)
			odf_header.read_bin_stream(fp_bin_odf)
			
			if not (odf_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")

			recno = odf_header.get_recno()
			
			# check if the header record we read was stored.
			if recno == 0:
				continue

			self.d_recno_index[recno] = odf_header.to_dict()
			#log.debug(odf_header.d_fields)
		# Parse body
		try:
			while True:
				odf_body = ODFBody()
				odf_body.read_bin_stream(fp_bin_odf)
				# Check if record size is correct (42 bytes)
				if not (odf_body.get_size() == self.record_size):
					raise ODFException("Failed ODF Record Integrity Test.")
				# Skip null records.
				recno = odf_body.get_recno()
				if recno == 0:
					continue
				self.d_recno_index[recno] = odf_body.to_dict()
		except ODFEOF as err:
			# EOF breaks the loop
			log.debug("EOF detected")
			pass
		except:
			raise


	def read_text_stream(self, fp_txt_odf):
		""" Read and parse ODF from text stream. Also eliminate duplicate records.
		For duplicates, the most recently seen record overrides previous ones.
		@param fp_txt_odf: Text stream
		"""

		# Parse headers
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for i in range(len(self.ld_header_layout)):
			d_header = self.ld_header_layout[i]
			s_hdr_name = list(d_header.keys())[0]
			d_hdr_param = list(d_header.values())[0]
			# Skip over header records that are not in text file.
			if not d_hdr_param['b_text']:
				continue
			#if not d_hdr_param['b_text'] and not self.b_fill_missing_headers:
			#	continue

			odf_header = ODFHeader(**d_hdr_param)
			odf_header.read_text_stream(fp_txt_odf)
			
			if not (odf_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")

			recno = odf_header.get_recno()
			
			# check if the header record we read was stored.
			if recno == 0:
				continue

			self.d_recno_index[recno] = odf_header.to_dict()

		try:
			# Parse body
			while True:
				odf_body = ODFBody()
				odf_body.read_text_stream(fp_txt_odf)
				# Check if record size is correct (42 bytes)
				if not (odf_body.get_size() == self.record_size):
					raise ODFException("Failed ODF Record Integrity Test.")
				# Skip null records.
				recno = odf_body.get_recno()
				if recno == 0:
					log.debug("Null record in text file")
					raise ODFException("Null record in text file")
				self.d_recno_index[recno] = odf_body.to_dict()
		except ODFEOF as err:
			# EOF breaks the loop
			pass
		except:
			raise

	def to_bin(self):
		""" Pack this ODF into its binary format.
		"""
		
		buf = b''
		null_odf_rec = ODFBody()
		l_recnos = sorted(list(self.d_recno_index.keys()))
		final_recno = l_recnos[-1]
		expected_buf_len = final_recno * self.record_size
		for current_recno in range(1, final_recno+1):

			if current_recno in self.d_recno_index:
				d_odf_rec = self.d_recno_index[current_recno]
				if self.is_header_recno(current_recno):
					d_hdr_param = list(self.ld_header_layout[current_recno-1].values())[0]
					hdr_value = d_odf_rec['ODF_OPEN']
					odf_header = ODFHeader(value=hdr_value, **d_hdr_param)
					buf = buf + odf_header.to_bin()
				else:
					odf_body = ODFBody(d_fields=d_odf_rec)
					buf = buf + odf_body.to_bin()
			else:
				buf = buf + null_odf_rec.to_bin()

		if not len(buf) == expected_buf_len:
			raise ODFException("Error encoding Odf to binary")

		return buf

	def to_bin_file(self, s_odf_bin):
		fp_odf_bin = open(s_odf_bin, "wb")
		buf = self.to_bin()
		fp_odf_bin.write(buf)
		fp_odf_bin.close()

	def is_header_recno(self, recno):
		return (recno <= len(self.ld_header_layout))

	def to_dict(self, s_odf_basename):
		""" Create a list of dicts to write to DD. Does deduplication along the fly.
		"""
		ld_odf_recs = []
		l_recnos = sorted(list(self.d_recno_index.keys()))

		for recno in l_recnos:
			d_odf_rec = self.d_recno_index[recno]
			if self.is_header_recno(recno):
				hdr_value = list(d_odf_rec.values())[1]
				d_hdr_param = list(self.ld_header_layout[recno].values())[0]
				odf_header = ODFHeader(value=hdr_value, **d_hdr_param)
				d_odf_rec = odf_header.to_dict()
			d_odf_rec['ODF_NAME'] = s_odf_basename
			ld_odf_recs.append(d_odf_rec)

		return ld_odf_recs

	def __repr__(self):
		""" Return a text representation of the ODF.
		"""
		buf = ""
		l_recnos = sorted(list(self.d_recno_index.keys()))
		for recno in l_recnos:
			d_odf_rec = self.d_recno_index[recno]
			if self.is_header_recno(recno):
				hdr_value = list(d_odf_rec.values())[1]
				s_hdr = "%d\n" % int(hdr_value)
				buf = buf + s_hdr
			else:
				s_body = "%d, %s, %s, %s, %s, %d\n" % (
						int(d_odf_rec['ODF_RECNO']),
						str(d_odf_rec['ODF_OPEN']),
						str(d_odf_rec['ODF_HIGH']),
						str(d_odf_rec['ODF_LOW']),
						str(d_odf_rec['ODF_CLOSE']),
						int(d_odf_rec['ODF_VOLUME']),
					)
				buf = buf + s_body
		return buf


	def get_highest_recno(self):
		l_recnos = sorted(list(self.d_recno_index.keys()))
		return l_recnos[-1]


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

			odf_h_open = odf_h["ODF_OPEN"]
			odf_h_high = odf_h["ODF_HIGH"]
			odf_h_low = odf_h["ODF_LOW"]
			odf_h_close = odf_h["ODF_CLOSE"]
			
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
		if header_storloc not in self.d_recno_index:
			return dc.Decimal('0')
		log.debug("Fetching header %d" % int(header_storloc))
		d_header_rec = self.d_recno_index[int(header_storloc)]
		return d_header_rec['ODF_OPEN']

	def add_missing_record(self, recno, dc_open, dc_high, dc_low, dc_close, dc_volume=dc.Decimal('0')):

		odf_record = ODFBody(d_fields={
				'ODF_RECNO' : recno,
				'ODF_OPEN' : dc_open,
				'OPEN_HIGH' : dc_high,
				'OPEN_LOW' : dc_low,
				'OPEN_CLOSE' : dc_close,
				'OPEN_VOLUME' : dc_volume,
			})

		if recno in self.d_recno_index:
			raise ODFException("Record at %d already exists. Cannot fill missing." % recno)
		self.d_recno_index[recno] = odf_record.to_dict()
		#self.l_odf_body.append(odf_record)

	def add_missing_header(self, header_storloc, dc_header_value):
		d_header_layout = self.ld_header_layout[int(header_storloc)-1]
		d_header_kwargs = list(d_header_layout.values())[0]
		#s_header_name = list(d_header_layout.keys())[0]
		#s_header_storloc = "_".join([s_header_name, "STORLOC"])
		
		odf_hdr_rec = ODFHeader(value=dc_header_value, **d_header_kwargs)

		#self.l_odf_headers.append(odf_hdr_rec)
		self.d_recno_index[int(header_storloc)] = odf_hdr_rec.to_dict()


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
		#r = self.l_odf_body[-1].get_recno()
		l_recnos = sorted(list(self.d_recno_index.keys()))
		r = l_recnos[-1]
		while self.is_recno_out_of_limits(r, trading_start_recno, trading_recs_perday):
			r -= 1

		return r
		
	def find_first_non_zero_open(self, trading_start_recno):
		r = trading_start_recno
		dc_odf_open = dc.Decimal('0')
		while True:
			if r not in self.d_recno_index:
				return dc.Decimal('0')
			odf_rec = self.d_recno_index[r]

			dc_odf_open = odf_rec['ODF_OPEN']

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
