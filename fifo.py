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
import math

# Create logger
import logging
log = logging.getLogger(__name__)

class FIFOHeader(BinaryStruct):
	""" FIFO Header Binary Struct.
	"""

	def __init__(self, ld_fields, padding, storloc, b_text, fn_parse, d_fields=None):
		""" Constructor.
		"""
		self.ld_fields = ld_fields
		self.storloc = storloc
		self.b_text = b_text
		self.fn_parse = fn_parse
		super(FIFOHeader, self).__init__(d_fields, padding)
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

	
	def get_storloc(self):
		return self.storloc

	def get_recno(self):
		return self.get_storloc()

	def __repr__(self):
		""" Return a text representation of this object
		"""
		ls_values = []
		for d_field in self.ld_fields:
			s_field = list(d_field.keys())[0]
			ls_values.append(str(self.d_fields[s_field]))
		# Return only the main field value, and not padding or 'storloc'
		s_buf = ','.join(ls_values) + '\n'
		return s_buf

	def to_dict(self, s_odf_basename):

		s_header_name = self.get_header_name()
		f_header_val = self.get_header_value()
		fifo_recno = self.get_storloc()

		ohlc_divider = self.get_field('OHLC_DIVIDER')
		tick = self.get_field('TICK')

		d_fifo_hdr_rec = {
			'ODF_NAME': s_odf_basename,
			'FIFO_RECNO' : fifo_recno,
			'OHLC_DIVIDER': ohlc_divider,
			'TICK': tick,
		}

		return d_fifo_hdr_rec

class FIFORecord(BinaryStruct):
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

	def __init__(self, d_fifo_rec=None):
		""" Constructor
		"""
		super(FIFORecord, self).__init__(d_fields=d_fifo_rec)

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
			l_values.append(Decimal(ls_values[1]))	# FIFO_OPEN
			l_values.append(Decimal(ls_values[2]))	# FIFO_HIGH
			l_values.append(Decimal(ls_values[3]))	# FIFO_LOW
			l_values.append(Decimal(ls_values[4]))	# FIFO_CLOSE

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
			s_field = list(d_field.keys())[0]
			ls_values.append(str(self.d_fields[s_field]))
		s_buf = ','.join(ls_values) + '\n'
		return s_buf

class FIFO(BinaryStruct):
	""" FIFO File converter, and in-memory representation.
	"""

	""" The header layout specifies the locations and contents
	of each individual header record in the FIFO. The contents
	of each entry provide arguments to the FIFOHeader Constructor
	for each header record.
	"""
	ld_header_layout = [
		{
			'FIFO_HEADER' : {
				'ld_fields' : [
					# {'<field_name>' : 'x/c/...' },
					{'FIFO_HEADER_STORLOC' : 'H'},
					{'OHLC_DIVIDER' : 'd'},
					{'TICK' : 'd'},
				],
				'padding' : 16,
				'storloc' : 201,
				'b_text' : True, 
				'fn_parse' : dc.Decimal,
			},
		},
		
	]

	FIFO_COUNT=200

	def __init__(self, fifo_count=None):
		""" Constructor
		"""
		self.fifo_count = fifo_count
		if fifo_count is None:
			self.fifo_count = self.FIFO_COUNT
		
		self.l_fifo_headers = []
		
		for d_header in self.ld_header_layout:
			s_hdr_name = list(d_header.keys())[0]
			fifo_header = FIFOHeader(**d_header[s_hdr_name])
			self.l_fifo_headers.append(fifo_header)

		self.l_fifo_records = []
		self.d_recno_index = {}
		# Each binary FIFO record is 34 bytes long.
		self.record_size = 34

	def validate_headers(self):
		recno_count = self.fifo_count + 1
		
		for i, fifo_header in enumerate(self.l_fifo_headers):
			if not (i == recno_count-1-self.fifo_count):
				raise ODFException("Failed Header Integrity Test.")
			recno = fifo_header.get_recno()
			if not (recno == recno_count):
				raise ODFException("Failed Header Integrity Test.")
			recno_count += 1

		if not (recno_count == self.fifo_count+2):
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

	def read_list(self, l_fifo_arr):
		num_recs = len(l_fifo_arr)

		d_dedup_dict = {}

		for i in range(num_recs):
			fifo_rec = FIFORecord(l_fifo_arr[i])
			if not (fifo_rec.get_size() == self.record_size):
					raise ODFException("Failed Record Integrity Test.")
			recno = fifo_rec.get_recno()
			if recno == 0:
				continue
			elif recno == self.fifo_count+1:
				break
			self.d_recno_index[recno] = fifo_rec
			self.dedup(d_dedup_dict, fifo_rec)

		self.l_fifo_records = self.get_dedup_objs(d_dedup_dict)	

		self.store_header()

	def update_from_list(self, l_fifo_arr):
		
		for d_new_fifo in l_fifo_arr:
			recno = d_new_fifo['FIFO_RECNO']
			if recno in self.d_recno_index:
				fifo_rec = self.d_recno_index[recno]
				fifo_rec.update_fields(d_new_fifo)

	def read_bin_stream(self, fp_bin_fifo):
		""" Read and parse FIFO from a binary stream. Also eliminate duplicate records.
		For duplicates, the most recently seen record overrides previous ones.
		@param fp_bin_odf: Binary stream.
		"""

		# Parse body
		d_dedup_dict = {}
		try:
			while True:
				fifo_rec = FIFORecord()
				fifo_rec.read_bin_stream(fp_bin_fifo)
				# Check if record size is correct (42 bytes)
				if not (fifo_rec.get_size() == self.record_size):
					raise ODFException("Failed Record Integrity Test.")
				recno = fifo_rec.get_recno()
				if recno == 0:
					log.debug("recno=0 encountered")
					continue
				self.d_recno_index[recno] = fifo_rec
				self.dedup(d_dedup_dict, fifo_rec)
				log.debug(str(fifo_rec))
				if recno == self.fifo_count:
					break
				#log.debug(odf_body.get_field('ODF_RECNO'))
		except ODFEOF as err:
			# EOF breaks the loop.
			pass
		except:
			raise

		self.l_fifo_records = self.get_dedup_objs(d_dedup_dict)	
		log.debug(self.l_fifo_records)
		# Parse headers.
		# To do: store odf records as a list of dicts in internally instead of 
		# storing the encoder/decoder binarystruct objects
		for fifo_header in self.l_fifo_headers:
			fifo_header.read_bin_stream(fp_bin_fifo)
			# Check if record size is correct (42 bytes)
			if not (fifo_header.get_size() == self.record_size):
				raise ODFException("Failed Header Integrity Test.")
			recno = fifo_header.get_recno()
			self.d_recno_index[recno] = fifo_header

		self.validate_headers()


	def to_bin(self):
		""" Pack this FIFO into its binary format.
		"""
		# First hash all records into the dup-detect dict
		first_recno = 1
		last_recno = self.l_fifo_records[-1].get_recno()
		fifo_index = 0
		
		null_fifo_rec = FIFORecord()
		buf = b''
		# Store each record at byte location ((ODF_RECNO-1) * 42)+1
		# Store missing records as null entries.
		for recno in range(first_recno, last_recno+1):
			current_valid_recno = self.l_fifo_records[fifo_index].get_recno()
			fifo_rec = None
			if recno == current_valid_recno:
				fifo_rec = self.l_fifo_records[fifo_index]
				fifo_index += 1
			else:
				fifo_rec = null_fifo_rec
			buf = buf + fifo_rec.to_bin()
			log.debug(str(fifo_rec))
		header_recno = self.fifo_count + 1
		# First hash all headers into the dup-detect dict
		for fifo_header in self.l_fifo_headers:
			buf = buf + fifo_header.to_bin()
			log.debug(str(fifo_header))
		log.debug(len(buf))
		return buf

	def to_dict(self, s_odf_basename):
		""" Create a list of dicts to write to DD. 
		"""
		ld_fifo_recs = []

		for fifo_rec in self.l_fifo_records:
			ld_fifo_recs.append(fifo_rec.to_dict(s_odf_basename))

		for fifo_header in self.l_fifo_headers:
			ld_fifo_recs.append(fifo_header.to_dict(s_odf_basename))

		return ld_fifo_recs

	def to_bin_file(self, s_fifo_bin):
		fp_fifo_bin = open(s_fifo_bin, "wb")
		buf = self.to_bin()
		fp_fifo_bin.write(buf)
		fp_fifo_bin.close()

	def __repr__(self):
		""" Return a text representation of the FIFO.
		"""
		buf = ""

		for fifo_rec in self.l_fifo_records:
			buf = buf + str(fifo_rec)
		
		# In a FIFO, the header trails the records.
		for fifo_header in self.l_fifo_headers:
			buf = buf + str(fifo_header)
		
		return buf

	def compute_ohlc_divider(self, ctx):

		sum_of_digits = 0
		num_rec = len(self.l_fifo_records)
		for fifo_rec in self.l_fifo_records:

			dc_open = fifo_rec.get_field('FIFO_OPEN')
			dc_frac = dc_open % 1
			(sign, l_digits, expnt) = dc_frac.as_tuple()
			sum_of_digits += len(l_digits)

		d = sum_of_digits / num_rec
		md = sum_of_digits // num_rec

		if md + 0.05 < d:
			md += 1

		ohlc_divider = ctx.power(dc.Decimal('10'), dc.Decimal(str(md)))

		return ohlc_divider

	def compute_tick(self, ctx, dc_ohlc_divider):
		dc_tick = dc.Decimal('0')

		fifo_rec = self.l_fifo_records[0]

		dc_open_prev = fifo_rec.get_field('FIFO_OPEN')
		dc_high_prev = fifo_rec.get_field('FIFO_HIGH')
		dc_low_prev = fifo_rec.get_field('FIFO_LOW')
		dc_close_prev = fifo_rec.get_field('FIFO_CLOSE')

		dc_open_prev = ctx.multiply(dc_open_prev, dc_ohlc_divider)
		dc_high_prev = ctx.multiply(dc_high_prev, dc_ohlc_divider)
		dc_low_prev = ctx.multiply(dc_low_prev, dc_ohlc_divider)
		dc_close_prev = ctx.multiply(dc_close_prev, dc_ohlc_divider)

		for fifo_rec in self.l_fifo_records:
			dc_open = fifo_rec.get_field('FIFO_OPEN')
			dc_high = fifo_rec.get_field('FIFO_HIGH')
			dc_low = fifo_rec.get_field('FIFO_LOW')
			dc_close = fifo_rec.get_field('FIFO_CLOSE')

			dc_open = ctx.multiply(dc_open, dc_ohlc_divider)
			dc_high = ctx.multiply(dc_high, dc_ohlc_divider)
			dc_low = ctx.multiply(dc_low, dc_ohlc_divider)
			dc_close = ctx.multiply(dc_close, dc_ohlc_divider)

			dc_diff1 = ctx.abs(ctx.subtract(dc_open, dc_open_prev))
			dc_diff2 = ctx.abs(ctx.subtract(dc_high, dc_high_prev))
			dc_diff3 = ctx.abs(ctx.subtract(dc_low, dc_low_prev))
			dc_diff4 = ctx.abs(ctx.subtract(dc_close, dc_close_prev))
			
			if not dc_diff1 == 0 and dc_tick > dc_diff1:
				dc_tick = dc_diff1
			if not dc_diff2 == 0 and dc_tick > dc_diff2:
				dc_tick = dc_diff2
			if not dc_diff3 == 0 and dc_tick > dc_diff3:
				dc_tick = dc_diff3
			if not dc_diff4 == 0 and dc_tick > dc_diff4:
				dc_tick = dc_diff4

			dc_open_prev = dc_open
			dc_high_prev = dc_high
			dc_low_prev = dc_low
			dc_close_prev = dc_close

		return dc_tick

	def store_header(self):
		ctx = dc.Context()

		dc_ohlc_divider = self.compute_ohlc_divider(ctx)

		dc_tick = self.compute_tick(ctx, dc_ohlc_divider)

		d_fifo_hdr = {
				"FIFO_RECNO" : self.fifo_count+1,
				"OHLC_DIVIDER" : dc_ohlc_divider,
				"TICK" : dc_tick,
		}

		ld_fields = self.ld_header_layout[0]['FIFO_HEADER']
		fifo_header = FIFOHeader(d_fields=d_fifo_hdr, **ld_fields)

		self.l_fifo_headers = [fifo_header]
		recno = fifo_header.get_recno()
		self.d_recno_index[recno] = fifo_header

	def get_tick(self):

		fifo_header_rec = self.l_fifo_headers[0]

		return fifo_header_reck.get_field('TICK')

	def get_ohlc_divider(self):

		fifo_header_rec = self.l_fifo_headers[0]

		return fifo_header_reck.get_field('OHLC_DIVIDER')

def open_fifo_bin(s_fifo_bin):
	fifo_obj = FIFO()
	fp_fifo_bin = open(s_fifo_bin, "rb")
	fifo_obj.read_bin_stream(fp_fifo_bin)
	fp_fifo_bin.close()
	return fifo_obj