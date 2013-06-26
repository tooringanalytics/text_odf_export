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

from odfexcept import *
from binary import BinaryStruct
import decimal as dc
import re

import logging
log = logging.getLogger(__name__)

class ChunkHeader(BinaryStruct):
	ld_fields = [
			# {'<field_name>' : 'x/c/...' },
			{'LOWEST_LOW' : 'd'},
			{'VOLUME_TICK' : 'H'},
			{'CHUNK_OPEN_RECNO' : 'H'},
			{'CHUNK_CLOSE_RECNO' : 'H'},
		]

	def __init__(self, *kargs, **kwargs):
		super(ChunkHeader, self).__init__(*kargs, **kwargs)

	def __repr__(self):
		f_lowest_low = self.get_field('LOWEST_LOW')
		volume_tick = self.get_field('VOLUME_TICK')
		chunk_open_recno = self.get_field('CHUNK_OPEN_RECNO')
		chunk_close_recno = self.get_field('CHUNK_CLOSE_RECNO')

		buf = "%f,%d,%d,%d" % (f_lowest_low, volume_tick, chunk_open_recno, chunk_close_recno)

		return buf

	def to_dict(self):
		""" Return dictionary representation 
		"""
		f_lowest_low = self.get_field("LOWEST_LOW")
		volume_tick = self.get_field("VOLUME_TICK")
		chunk_open_recno = self.get_field("CHUNK_OPEN_RECNO")
		chunk_close_recno = self.get_field("CHUNK_CLOSE_RECNO")
		
		d_chunk_rec = {
					'LOWEST_LOW': f_lowest_low,
					'VOLUME_TICK' : volume_tick,
					'CHUNK_OPEN_RECNO' : chunk_open_recno,
					'CHUNK_CLOSE_RECNO' : chunk_close_recno,
					}

		return d_chunk_rec

class ShortChunkHeader(BinaryStruct):
	
	ld_fields = [
			# {'<field_name>' : 'x/c/...' },
			{'LOWEST_LOW' : 'L'},
			{'VOLUME_TICK' : 'H'},
			{'CHUNK_OPEN_RECNO' : 'H'},
			{'CHUNK_CLOSE_RECNO' : 'H'},
		]

	def __init__(self, *kargs, **kwargs):
		super(ShortChunkHeader, self).__init__(*kargs, **kwargs)

	def __repr__(self):
		lowest_low = self.get_field('LOWEST_LOW')
		volume_tick = self.get_field('VOLUME_TICK')
		chunk_open_recno = self.get_field('CHUNK_OPEN_RECNO')
		chunk_close_recno = self.get_field('CHUNK_CLOSE_RECNO')

		buf = "%d,%d,%d,%d" % (lowest_low, volume_tick, chunk_open_recno, chunk_close_recno)

		return buf

	def to_dict(self):
		""" Return dictionary representation 
		"""
		lowest_low = self.get_field("LOWEST_LOW")
		volume_tick = self.get_field("VOLUME_TICK")
		chunk_open_recno = self.get_field("CHUNK_OPEN_RECNO")
		chunk_close_recno = self.get_field("CHUNK_CLOSE_RECNO")
		
		d_chunk_rec = {
					'LOWEST_LOW': lowest_low,
					'VOLUME_TICK' : volume_tick,
					'CHUNK_OPEN_RECNO' : chunk_open_recno,
					'CHUNK_CLOSE_RECNO' : chunk_close_recno,
					}

		return d_chunk_rec
	
class Chunk(BinaryStruct):

	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'OPEN' : 'd'},
		{'HIGH' : 'd'},
		{'LOW' : 'd'},
		{'CLOSE' : 'd'},
		{'VOLUME' : 'd'},
	]

	def __init__(self, *kargs, **kwargs):
		super(Chunk, self).__init__(*kargs, **kwargs)
		
		
	def to_dict(self):
		""" Return dictionary representation 
		"""
		f_open = self.get_field("OPEN")
		f_high = self.get_field("HIGH")
		f_low = self.get_field("LOW")
		f_close = self.get_field("CLOSE")
		f_volume = self.get_field("VOLUME")

		d_chunk_rec = {
					'OPEN': f_open,
					'HIGH' : f_high,
					'LOW' : f_low,
					'CLOSE' : f_close,
					'VOLUME' : f_volume,
					}

		return d_chunk_rec

class ShortChunk(BinaryStruct):
	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'OPEN' : 'H'},
		{'HIGH' : 'H'},
		{'LOW' : 'H'},
		{'CLOSE' : 'H'},
		{'VOLUME' : 'H'},
	]

	def __init__(self, *kargs, **kwargs):
		super(ShortChunk, self).__init__(*kargs, **kwargs)

	def to_dict(self):
		""" Return dictionary representation 
		"""
		h_open = self.get_field("OPEN")
		h_high = self.get_field("HIGH")
		h_low = self.get_field("LOW")
		h_close = self.get_field("CLOSE")
		h_volume = self.get_field("VOLUME")

		d_chunk_rec = {
					'OPEN': h_open,
					'HIGH' : h_high,
					'LOW' : h_low,
					'CLOSE' : h_close,
					'VOLUME' : h_volume,
					}

		return d_chunk_rec

	def __repr__(self):
		h_open = self.get_field("OPEN")
		h_high = self.get_field("HIGH")
		h_low = self.get_field("LOW")
		h_close = self.get_field("CLOSE")
		h_volume = self.get_field("VOLUME")

		buf = "%d,%d,%d,%d,%d" % (h_open, h_high, h_low, h_close, h_volume)

		return buf

class ChunkArray(object):

	def __init__(self, s_chunk_file_name=None, chunk_size=0, debug_id=0):
		self.s_chunk_file_name = s_chunk_file_name
		self.d_chunk_arr = {}
		self.chunk_size= chunk_size
		self.long_chunk_size = 40
		self.short_chunk_size = 10
		self.s_file = None
		self.debug_id = debug_id
	
	def get_name(self):
		return self.s_chunk_file_name

	def set_field(self, recno, s_field_name, value):
		if recno not in self.d_chunk_arr:
			self.d_chunk_arr[recno] = {}
		self.d_chunk_arr[recno][s_field_name] = value

	def get_field(self, recno, s_field_name):
		try:
			return self.d_chunk_arr[recno][s_field_name]
		except:
			log.error(self.get_name())
			log.error(self.s_file)
			log.error(self.debug_id)
			log.error(recno)
			log.error(s_field_name)
			#log.error(self.d_chunk_arr[recno])
			#log.debug(self.to_csv())
			raise

	def get_header_field(self, s_hdr_field_name):
		val = self.d_chunk_arr[self.chunk_size+1][s_hdr_field_name]
		#log.debug(val)
		return val

	def set_header_field(self, s_hdr_field_name, value):
		self.d_chunk_arr[self.chunk_size+1][s_hdr_field_name] = value

	def set_header(self, d_chunk_header):
		#log.debug(d_chunk_header)
		self.d_chunk_arr[self.chunk_size+1] = d_chunk_header

	def highest_volume(self):
		highest_volume = 0
		for i in range(self.chunk_size):
			vol = self.get_field(i+1, 'VOLUME')
			if vol > highest_volume:
				highest_volume = vol

		return highest_volume

	def lowest_low(self):
		lowest_low = 999999999
		for i in range(self.chunk_size):
			low = self.get_field(i+1, 'LOW')
			if low > 0 and low < lowest_low:
				lowest_low = low
		if lowest_low == 999999999:
			lowest_low = 0
		'''
		if lowest_low == 0:
			log.error(self.get_name())
			log.error(self.to_csv())
			raise ODFException("Invalid lowest_low")
		'''
		return lowest_low

	def to_bin_short(self, key=None):
		buf = b''
		null_chunk_rec = ShortChunk()
		for recno in range(1, self.chunk_size+1):
			if recno in self.d_chunk_arr:
				d_fields = self.d_chunk_arr[recno]
				chunk_rec = ShortChunk(d_fields=d_fields)
				try:
					buf = buf + chunk_rec.to_bin(key)
				except:
					log.error(chunk_rec.d_fields)
					log.error(self.get_name())
					log.error(self.to_csv())
					raise
			else:
				buf = buf + null_chunk_rec.to_bin(key)
		d_hdr_fields = self.d_chunk_arr[self.chunk_size + 1]
		chunk_hdr_rec = ShortChunkHeader(d_fields=d_hdr_fields)
		buf += chunk_hdr_rec.to_bin(key)

		return buf
	
	def to_csv(self):
		buf = ''
		#log.debug("Chunk size: %d records" % self.length())
		for recno in range(1, self.chunk_size+1):
			if recno in self.d_chunk_arr:
				d_fields = self.d_chunk_arr[recno]
				chunk_rec = ShortChunk(d_fields=d_fields)
				buf = buf + str(chunk_rec) + '\n'

		d_hdr_fields = self.d_chunk_arr[self.chunk_size + 1]
		chunk_hdr_rec = ShortChunkHeader(d_fields=d_hdr_fields)
		buf += str(chunk_hdr_rec) + '\n'
		return buf
	
	def to_bin_file_short(self, s_chunk_file_local_path, key=None):
		fp_chunk = open(s_chunk_file_local_path, "wb")
		buf = self.to_bin_short(key)
		fp_chunk.write(buf)
		fp_chunk.close()
		
	def save_csv(self, s_chunk_csv_file_name):
		#log.debug("saving chunk to csv: %s" % s_chunk_csv_file_name)
		fp = open(s_chunk_csv_file_name, "w")
		buf = self.to_csv()
		#log.debug(buf)
		fp.write(buf)
		fp.close()

	def to_bin_long(self, key=None):

		buf = b''
		null_chunk_rec = Chunk()
		for recno in range(1, self.chunk_size+1):
			if recno in self.d_chunk_arr:
				d_fields = self.d_chunk_arr[recno]
				chunk_rec = Chunk(d_fields=d_fields)
				buf += chunk_rec.to_bin(key)
			else:
				buf = buf + null_chunk_rec.to_bin(key)

		d_hdr_fields = self.d_chunk_arr[self.chunk_size + 1]
		chunk_hdr_rec = ChunkHeader(d_fields=d_hdr_fields)
		buf += chunk_hdr_rec.to_bin(key)

		return buf

	def read_bin_long(self, fp_bin, key=None):
		chunk_size = self.chunk_size
		for i in range(chunk_size):
			chunk = Chunk()
			chunk.read_bin_stream(fp_bin, key)
			# Check if record size is correct (40 bytes)
			if not (chunk.get_size() == self.long_chunk_size):
				raise ODFException("Failed Chunk Integrity Test.")

			self.d_chunk_arr[i+1] = chunk.to_dict()
		chunk_hdr_rec = ChunkHeader()
		chunk_hdr_rec.read_bin_stream(fp_bin, key)
		self.d_chunk_arr[self.chunk_size+1] = chunk_hdr_rec.to_dict()	
		
		if 'VOLUME_TICK' not in self.d_chunk_arr[self.chunk_size+1]:
			raise ODFException("Failed integrity test.")	

	def read_bin_short(self, fp_bin, key):
		chunk_size = self.chunk_size
		for i in range(chunk_size):
			chunk = ShortChunk()
			chunk.read_bin_stream(fp_bin, key)
			# Check if record size is correct (10 bytes)
			if not (chunk.get_size() == self.short_chunk_size):
				raise ODFException("Failed ShortChunk Integrity Test.")

			self.d_chunk_arr[i+1] = chunk.to_dict()

		chunk_hdr_rec = ShortChunkHeader()
		chunk_hdr_rec.read_bin_stream(fp_bin, key)
		self.d_chunk_arr[self.chunk_size+1] = chunk_hdr_rec.to_dict()		
		if 'VOLUME_TICK' not in self.d_chunk_arr[self.chunk_size+1]:
			raise ODFException("Failed integrity test.")	

	def length(self):
		return len(self.d_chunk_arr)

class ChunkArrayList(object):

	def __init__(self):
		self.d_chunk_array = {}
		self.l_chunk_list = []

	def add_chunk_arr(self, chunk_array):
		self.d_chunk_array[chunk_array.get_name()] = chunk_array
		self.l_chunk_list.append(chunk_array)

	def get_chunk_arr(self, s_chunk_file_name):
		if s_chunk_file_name in self.d_chunk_array:
			return self.d_chunk_array[s_chunk_file_name]
		return None

	def get_chunk_arr_at(self, index):
		return self.l_chunk_list[index]

	def length(self):
		return len(self.l_chunk_list)

def read_short_chunk_array(s_chunk_file_path, s_chunk_file_name, chunk_size, key=None):

	#log.debug(s_chunk_file_name)
	fp_chunk = open(s_chunk_file_path, "rb")

	chunk_array = ChunkArray(s_chunk_file_name, chunk_size, debug_id=1)

	chunk_array.read_bin_short(fp_chunk, key)

	fp_chunk.close()
	
	chunk_array.s_file = s_chunk_file_name
	
	return chunk_array

def make_chunk_file_name(L_no, fce_jsunnoon, chunk_no, s_odf_basename):
	
	#log.debug(s_odf_basename)
	matchobj = re.match(r'^(.*)\-(\d+)$', s_odf_basename)

	s_prefix = matchobj.group(1)

	s_suffix = ''.join([str(L_no), str(fce_jsunnoon), "%02d" % chunk_no])

	s_chunk_file_name = '-'.join([s_prefix, s_suffix])

	s_chunk_file_name = '.'.join([s_chunk_file_name, 'fce'])

	#log.debug("Generated chunk file name: %s" % s_chunk_file_name)
	return s_chunk_file_name

def get_components_from_chunk_name(s_chunk_name):
	#log.debug(s_chunk_name)
	# jsunnoon is 5-digit julian day no., chunk_no is zero-padded 2-digit integer
	matchobj = re.match(r'^(.*)\-(\d+)(\d\d\d\d\d)(\d\d)\.fce$', s_chunk_name)

	L_no = int(matchobj.group(2))
	fce_jsunnoon = int(matchobj.group(3))
	chunk_no = int(matchobj.group(4))

	return (L_no, fce_jsunnoon, chunk_no)	

