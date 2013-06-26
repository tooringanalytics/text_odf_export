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

import datetime as dt
import os.path
import re
import decimal as dc
import argparse
import sys
import glob
import math

from odfexcept import *
import config
import fifo
import odf
import fce
import chunk
import s3
import dd

import logging
import logging.handlers
log = logging.getLogger(__name__)


def rounddown(n):
	return math.floor(n)

def roundup(n):
	n = math.ceil(n)
	if n == 0:
		return 1
	return n

class Odf2Fce(object):
	""" Application class for text_odf_export
	"""

	re_reply_yes = re.compile(r'^(y|(yes))$', re.I)

	def __init__(self):
		""" Constructor
		"""
		self.parser = argparse.ArgumentParser(description='ODF to FCE Processor.')

	def arg_parse(self):
		""" Specify command line args, and parse the command line
		"""		
		parser = self.parser
		
		parser.add_argument('-a', '--ask',
							action='store_true',
	                   		help='Prompt for Y/N before running.')
		
		parser.add_argument('-s', '--show',
							action='store_true',
	                   		help='Show the name of each ODF as it is being processed.')
		
		parser.add_argument('-p', '--print-fifo',
							action='store_true',
	                   		help='Only print contents of each fifo to the log.')
		
		return parser.parse_args()

	def set_args(self, args):
		""" Set internal variables according to results parsed from command line
		@param args: argparse object containing parse results. 
		"""
		
		self.b_ask = True
		self.b_show = False

		if hasattr(args, "ask") and args.ask is not None:
			self.b_ask = args.ask
		
		if hasattr(args, "show") and args.show is not None:
			self.b_show = args.show
		
		if hasattr(args, "print_fifo") and args.print_fifo is not None:
			self.b_print_mode = args.print_fifo
		
	def prompt_interactive(self):
		""" Prompt the user on stdin to continue with the program.
		"""
		s_reply = ""
		
		sys.stdout.write("This program will process ODFs and generate FCEs. Proceed? (y/N): ")
		
		# Force the prompt to display
		sys.stdout.flush()

		try:
			s_reply = raw_input().strip()
		except NameError as err:
			s_reply = input().strip()
			pass
		except:
			raise
		#s_reply = input().strip()

		if not self.re_reply_yes.match(s_reply):
			sys.exit(-1)

	def get_working_dir(self):
		s_working_dir = os.path.dirname(os.path.abspath(__file__))
		return s_working_dir

	def refresh_fifo(self, odf_obj, s_fifo_dd, s_fifo_basename):
		config = self.config
		dd_store = self.dd_store
		fifo_obj = None
		if dd_store.fifo_exists(s_fifo_dd, s_fifo_basename):

			b_older_than_25_days = dd_store.is_fifo_older_than(s_fifo_dd, s_fifo_basename, 25)

			if b_older_than_25_days:
				log.debug("Updating FIFO (older than 25 days)...")

				l_fifo_arr = odf_obj.get_fifo_arr(config.fifo_count,
													config.trading_start_recno)
				
				fifo_obj = dd_store.open_fifo(s_fifo_dd, s_fifo_basename)
				fifo_obj.update_from_list(l_fifo_arr)
				fifo_obj.store_header()
				dd_store.save_fifo(s_fifo_dd, s_fifo_basename, fifo_obj)
			else:
				log.debug("FIFO is too recent (<25 days old). Skipping Update...")
				fifo_obj = dd_store.open_fifo(s_fifo_dd, s_fifo_basename)
		else:

			l_fifo_arr = odf_obj.get_fifo_arr(config.fifo_count,
												config.trading_start_recno)

			fifo_obj = fifo.FIFO(fifo_count=config.fifo_count)
			# Need to optimize to reduce writes. only updates entries should be saved back.
			fifo_obj.read_list(l_fifo_arr)
			dd_store.save_fifo(s_fifo_dd, s_fifo_basename, fifo_obj)

		return fifo_obj

	
	def print_fifo_local(self, odf_obj, s_fifo_dd):

		fifo_obj = fifo.FIFO()
		fp_fifo_dd = open(s_fifo_dd, "rb")
		fifo_obj.read_bin_stream(fp_fifo_dd)
		fp_fifo_dd.close()

		buf = str(fifo_obj)

		log.debug("FIFO %s TEXT:\n%s\n" % (s_fifo_dd, buf))

	def fill_missing_odf_header_records(self, odf_obj, s_odf_dd, s_odf_basename, fifo_obj, fce_pathspec):

		config = self.config
		dd_store = self.dd_store
		s3_store = self.s3_store

		# Get the common ODF headers
		odf_gmt_offset = odf_obj.get_header_value(config.gmt_offset_storloc)
		odf_trading_start_recno = odf_obj.get_header_value(config.trading_start_recno_storloc)
		odf_trading_recs_perday = odf_obj.get_header_value(config.trading_recs_perday_storloc)
		odf_idf_currency = odf_obj.get_header_value(config.idf_currency_storloc)
		odf_idf_currency_max_decimals = odf_obj.get_header_value(config.idf_currency_max_decimals_storloc)
		odf_split_factor = odf_obj.get_header_value(config.split_factor_storloc)
		odf_currency_value_of_point = odf_obj.get_header_value(config.currency_value_of_point_storloc)
	
		# Get the 'specific' odf headers, which may be missing
		odf_tick = odf_obj.get_header_value(config.tick_storloc)
		odf_ohlc_divider = odf_obj.get_header_value(config.ohlc_divider_storloc)
		odf_last_fced_recno = odf_obj.get_header_value(config.last_fced_recno_storloc)
		odf_highest_recno = odf_obj.get_header_value(config.highest_recno_storloc)
		odf_highest_recno_close = odf_obj.get_header_value(config.highest_recno_close_storloc)
		odf_prev_highest_recno_close = odf_obj.get_header_value(config.prev_highest_recno_close_storloc)

		# Save this ODF's common headers to public values
		config.gmt_offset = odf_gmt_offset
		config.trading_start_recno = odf_trading_start_recno
		config.trading_recs_perday = odf_trading_recs_perday
		config.idf_currency = odf_idf_currency
		config.idf_currency_max_decimals = odf_idf_currency_max_decimals
		config.split_factor = odf_split_factor
		config.currency_value_of_point = odf_currency_value_of_point

		# Check if the 'missing headers' are missing or have been added previously.
		if odf_tick == 0 or odf_ohlc_divider == 0 or odf_last_fced_recno == 0 or \
			odf_highest_recno == 0 or odf_highest_recno_close == 0 or \
			odf_prev_highest_recno_close == 0:			
			# Now prepare the missing ODF header values
			odf_tick = fifo_obj.get_tick()
			odf_ohlc_divider = fifo_obj.get_ohlc_divider()
			odf_last_fced_recno = 0
			odf_highest_recno = odf_obj.find_highest_recno(config.trading_start_recno, 
															config.trading_recs_perday)
			odf_highest_recno_close = odf_obj.get_field(odf_highest_recno, 'ODF_CLOSE')

			prev_fce_obj = s3_store.open_fce(fce_pathspec, fce_pathspec.s_prev_fce_header_file_name, config.encr_key)

			odf_prev_highest_recno_close = odf_obj.find_prev_highest_recno_close(prev_fce_obj)

			if odf_prev_highest_recno_close == 0:
				odf_prev_highest_recno_close = odf_obj.find_first_non_zero_open(config.trading_start_recno)

			# Fill missing headers in the ODF
			odf_obj.add_missing_header(config.tick_storloc, odf_tick)
			odf_obj.add_missing_header(config.ohlc_divider_storloc, odf_ohlc_divider)
			odf_obj.add_missing_header(config.last_fced_recno_storloc, odf_last_fced_recno)
			odf_obj.add_missing_header(config.highest_recno_storloc, odf_highest_recno)
			odf_obj.add_missing_header(config.highest_recno_close_storloc, odf_highest_recno_close)
			odf_obj.add_missing_header(config.prev_highest_recno_close_storloc, odf_prev_highest_recno_close)

			# Save the ODF
			dd_store.save_odf(s_odf_dd, s_odf_basename, odf_obj)

		# The missing headers either existed, or we just added them, so save them
		# to the public variable.
		config.tick = odf_tick
		config.ohlc_divider = odf_ohlc_divider
		config.last_fced_recno = odf_last_fced_recno
		config.highest_recno = odf_highest_recno
		config.highest_recno_close = odf_highest_recno_close
		config.prev_highest_recno_close = odf_prev_highest_recno_close

	def fill_missing_odf_records(self, odf_obj):
		config = self.config

		r = config.last_fced_recno
		c = odf_obj.get_value(r, 'ODF_CLOSE')

		if r == 0:
			r = config.trading_start_recno - 1
			c = config.prev_highest_recno_close

		log.debug("Filling missing ODF records starting at %d" % r)

		while True:

			r = r + 1
			
			if r > config.highest_recno:
				return True
			
			if odf_obj.is_recno_out_of_limits(r, config.trading_start_recno, config.trading_recs_perday):
				continue

			dc_odf_open = odf_obj.get_field(r, 'ODF_OPEN')
			dc_odf_high = odf_obj.get_field(r, 'ODF_HIGH')
			dc_odf_low = odf_obj.get_field(r, 'ODF_LOW')
			dc_odf_close = odf_obj.get_field(r, 'ODF_CLOSE')
			dc_odf_volume = odf_obj.get_field(r, 'ODF_VOLUME')

			if dc_odf_open == 0 or dc_odf_high == 0 or \
				dc_odf_low == 0 or dc_odf_close == 0:
				''' Fill the missing record
				''' 
				odf_obj.add_missing_record(r, 
											c, 
											c, 
											c, 
											c, 
											c)
				break
			else:
				''' Use this record's Close value to forward fill the next record.
				'''
				c = dc_odf_close

		return True

	def fill_fce_intervals_array(self):
		l_array = []
		config = self.config
		
		if config.b_process_L31:
			l_array.append([31, 1, 1, 0, 0])

		if config.b_process_L32:
			l_array.append([32, 2, 1, 0, 0])

		if config.b_process_L33:
			l_array.append([33, 3, 1, 0, 0])

		if config.b_process_L34:
			l_array.append([34, 4, 1, 0, 0])

		if config.b_process_L35:
			l_array.append([35, 5, 1, 0, 0])

		if config.b_process_L36:
			l_array.append([36, 7, 1, 0, 0])

		if config.b_process_L37:
			l_array.append([37, 10, 1, 0, 0])

		if config.b_process_L38:
			l_array.append([38, 15, 2, 0, 0])

		if config.b_process_L48:
			l_array.append([48, 30, 4, 0, 0])

		if config.b_process_L58:
			l_array.append([58, 60, 8, 0, 0])

		if config.b_process_L59:
			l_array.append([59, 60, 8, 30, 0])

		if config.b_process_L68:
			l_array.append([68, 240, 26, 0, 0])

		if config.b_process_L69:
			l_array.append([69, 240, 26, 30, 0])

		val_L79_4 = config.trading_start_recno - (config.trading_start_recno // 1440) * 1440
		l_array.append([79, 1440, 52, val_L79_4, 0])

		l_array.append([88, 10080, 260, 0, 0])

		l_array.append([98, 40320, 1040, 0, 1])

		return l_array

	def get_julian_day_number(self, dt_date):
		# Formula from: https://en.wikipedia.org/wiki/Julian_day#Finding_day_of_week_given_Julian_day_number
		# and : http://www.cs.utsa.edu/~cs1063/projects/Spring2011/Project1/jdn-explanation.html

		a = (14 - dt_date.month) // 12
		y = dt_date.year + 4800 - a
		m = dt_date.month + 12 * a - 3
		
		jdn = dt_date.day + (153 * m + 2) // 5 + 365 * y + (y // 4) - (y // 100) + (y // 400) - 32045

		return jdn

	def get_current_jsunnoon(self):
		
		today = dt.datetime.now()
		
		today_noon = dt.datetime(today.year, today.month, today.day, 12, 0)

		sunday_last_noon = today_noon - dt.timedelta(days=today_noon.weekday()) - dt.timedelta(days=1)

		# Return only least significant 5 digits of JDN
		return self.get_julian_day_number(sunday_last_noon) % 100000

	def write_chunk_files(self, fce_pathspec, chunk_arr_short_list):
		s3_store = self.s3_store
		config = self.config
		for i in range(chunk_arr_short_list.length()):
			chunk_arr_short = chunk_arr_short_list.get_chunk_arr_at(i)
			s_chunk_arr_short_name = chunk_arr_short.get_name()

			#s_chunk_file_name = re.sub(r'\.rs3', '.fce', s_chunk_arr_short_name)
			#s_chunk_file_name = '.'.join([s_chunk_arr_short_name, 'fce'])

			(L_no, fce_jsunnoon, chunk_no) = chunk.get_components_from_chunk_name(s_chunk_arr_short_name)

			(s_chunk_file_dir_s3, s_chunk_file_name_s3) = s3_store.get_chunk_file_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_arr_short_name)
			#s_chunk_file_name_s3 = os.sep.join([s_fce_s3, str(L_no), str(fce_jsunnoon), s_chunk_file_name])

			(s_chunk_file_tmp_dir, s_chunk_file_name_tmp) = s3_store.get_chunk_file_tmp_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_arr_short_name)
			#s_chunk_file_name_tmp = os.sep.join([s_fce_tmp, str(L_no), str(fce_jsunnoon), s_chunk_file_name])

			#log.debug("Writing chunk to TMP & S3: %s..." % s_chunk_file_name_tmp)
			s3_store.save_chunk_file(s_chunk_file_name_tmp, chunk_arr_short, config.encr_key)

			s_chunk_csv_name_tmp = '.'.join([s_chunk_file_name_tmp, "csv"])

			if self.config.b_do_csv_chunk:
				#log.debug("Saving chunk CSV: %s..." % (s_chunk_csv_name_tmp))
				chunk_arr_short.save_csv(s_chunk_csv_name_tmp)

			#log.debug("Moving chunk tmp file to S3 %s:%s:" % (s_chunk_file_dir_s3, s_chunk_file_name_s3))
			s3_store.upload_file(s_chunk_file_name_tmp, s_chunk_file_dir_s3, s_chunk_file_name_s3)

	def find_first_nonzero_chunk_open(self, chunk_arr):
		n = 0
		chunk_open = 0
		while True:
			
			n = n + 1
			
			chunk_open = chunk_arr.get_field(n, 'OPEN')

			if chunk_open > 0:
				break

			if n == self.config.chunk_size:
				return (self.config.chunk_size, 0)


		return (n, chunk_open)


	def find_last_nonzero_chunk_close(self, chunk_arr):
		n = self.config.chunk_size + 1
		chunk_close = 0
		while True:
			
			n = n -1
			
			chunk_close = chunk_arr.get_field(n, 'CLOSE')

			if chunk_close > 0:
				break

			if n == 1:
				return (1, 0)
				
		return (n, chunk_close)


	def fill_empty_chunk_records(self, chunk_arr):
		
		(first_nonzero_chunk_recno, first_nonzero_chunk_open) = self.find_first_nonzero_chunk_open(chunk_arr)

		m = first_nonzero_chunk_recno

		# First try back-fill, then fall back to forward-fill
		while True:
			m = m - 1		

			if m < 1:
				(last_nonzero_chunk_recno, last_nonzero_chunk_close) = self.find_last_nonzero_chunk_close(chunk_arr)
				m = last_nonzero_chunk_recno
				while True:
					m = m + 1
					if m > self.config.chunk_size:
						return chunk_arr
					else:
						chunk_arr.set_field(m, 'OPEN', last_nonzero_chunk_close)
						chunk_arr.set_field(m, 'HIGH', last_nonzero_chunk_close)
						chunk_arr.set_field(m, 'LOW', last_nonzero_chunk_close)
						chunk_arr.set_field(m, 'CLOSE', last_nonzero_chunk_close)
			else:
				chunk_arr.set_field(m, 'OPEN', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'HIGH', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'LOW', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'CLOSE', first_nonzero_chunk_open)


	def get_chunk_arr_short_list(self, chunk_arr_list):
		
		chunk_arr_short_list = chunk.ChunkArrayList()
		for i in range(chunk_arr_list.length()):
			chunk_arr = chunk_arr_list.get_chunk_arr_at(i)

			highest_volume = chunk_arr.highest_volume()
			#log.debug("HIGHEST_VOLUME=%d", int(highest_volume))
			volume_tick = roundup(highest_volume / 65535)
			#log.debug("VOLUME_TICK=%d", volume_tick)
			f_lowest_low = chunk_arr.lowest_low()

			chunk_arr.set_header_field('VOLUME_TICK', int(volume_tick))
			chunk_arr.set_header_field('LOWEST_LOW', f_lowest_low)

			chunk_arr = self.fill_empty_chunk_records(chunk_arr)

			chunk_arr_short = self.get_chunk_arr_short(chunk_arr)

			chunk_arr_short_list.add_chunk_arr(chunk_arr_short)

		return chunk_arr_short_list

	def get_chunk_arr_short(self, chunk_arr):

		chunk_arr_short = chunk.ChunkArray(chunk_arr.get_name(), self.config.chunk_size, debug_id=2)

		for i in range(self.config.chunk_size):
			vol_val = rounddown(chunk_arr.get_field(i+1, 'VOLUME') / chunk_arr.get_header_field('VOLUME_TICK'))
			open_val = rounddown(chunk_arr.get_field(i+1, 'OPEN') / float(self.config.tick))
			high_val = rounddown(chunk_arr.get_field(i+1, 'HIGH') / float(self.config.tick))
			low_val = rounddown(chunk_arr.get_field(i+1, 'LOW') / float(self.config.tick))
			close_val = rounddown(chunk_arr.get_field(i+1, 'CLOSE') / float(self.config.tick))

			chunk_arr_short.set_field(i+1, 'VOLUME', int(vol_val))
			chunk_arr_short.set_field(i+1, 'OPEN', int(open_val))
			chunk_arr_short.set_field(i+1, 'HIGH', int(high_val))
			chunk_arr_short.set_field(i+1, 'LOW', int(low_val))
			chunk_arr_short.set_field(i+1, 'CLOSE', int(close_val))

		d_chunk_header = {
			'LOWEST_LOW' : int(chunk_arr.get_header_field('LOWEST_LOW')),
			'VOLUME_TICK' : chunk_arr.get_header_field('VOLUME_TICK'),
			'CHUNK_OPEN_RECNO' : chunk_arr.get_header_field('CHUNK_OPEN_RECNO'),
			'CHUNK_CLOSE_RECNO' : chunk_arr.get_header_field('CHUNK_CLOSE_RECNO'),
		}

		chunk_arr_short.set_header(d_chunk_header)

		return chunk_arr_short


	def get_chunk_arr(self, chunk_arr_short):
		
		chunk_arr = chunk.ChunkArray(chunk_arr_short.get_name(), self.config.chunk_size, debug_id=3)
		for i in range(self.config.chunk_size):
			f_vol_val = chunk_arr_short.get_field(i+1, 'VOLUME') * chunk_arr_short.get_header_field('VOLUME_TICK')
			f_open_val = chunk_arr_short.get_field(i+1, 'OPEN') * float(self.config.tick)
			f_high_val = chunk_arr_short.get_field(i+1, 'HIGH') * float(self.config.tick)
			f_low_val = chunk_arr_short.get_field(i+1, 'LOW') * float(self.config.tick)
			f_close_val = chunk_arr_short.get_field(i+1, 'CLOSE') * float(self.config.tick)

			chunk_arr.set_field(i+1, 'VOLUME', f_vol_val)
			chunk_arr.set_field(i+1, 'OPEN', f_open_val)
			chunk_arr.set_field(i+1, 'HIGH', f_high_val)
			chunk_arr.set_field(i+1, 'LOW', f_low_val)
			chunk_arr.set_field(i+1, 'CLOSE', f_close_val)

		d_chunk_header = {
			'LOWEST_LOW' : chunk_arr_short.get_header_field('LOWEST_LOW') / float(self.config.ohlc_divider),
			'VOLUME_TICK' : chunk_arr_short.get_header_field('VOLUME_TICK'),
			'CHUNK_OPEN_RECNO' : chunk_arr_short.get_header_field('CHUNK_OPEN_RECNO'),
			'CHUNK_CLOSE_RECNO' : chunk_arr_short.get_header_field('CHUNK_CLOSE_RECNO'),
		}

		chunk_arr.set_header(d_chunk_header)

		return chunk_arr

	def store_zeros_in_chunk_arr(self, chunk_arr):
		#chunk_arr = ChunkArray(chunk_arr.get_name(), self.config.chunk_size)

		for i in range(self.config.chunk_size):
			chunk_arr.set_field(i+1, 'OPEN', 0.0)
			chunk_arr.set_field(i+1, 'HIGH', 0.0)
			chunk_arr.set_field(i+1, 'LOW', 0.0)
			chunk_arr.set_field(i+1, 'CLOSE', 0.0)
			chunk_arr.set_field(i+1, 'VOLUME', 0.0)
		
		d_chunk_header = {
			'LOWEST_LOW' : 0.0,
			'VOLUME_TICK' : int(0),
			'CHUNK_OPEN_RECNO' : int(self.config.highest_recno),
			'CHUNK_CLOSE_RECNO' : int(self.config.trading_start_recno),
		}

		chunk_arr.set_header(d_chunk_header)

		return chunk_arr

	def get_chunk_arr_ready(self, fce_pathspec, s_chunk_file_name, chunk_arr_list, L_no, fce_jsunnoon):
		
		s3_store = self.s3_store
		config = self.config
		
		chunk_size = self.config.chunk_size
		
		#log.debug("Looking for chunk array %s" % s_chunk_file_name)
		# First check if we already have the chunk array open in memory
		chunk_arr = chunk_arr_list.get_chunk_arr(s_chunk_file_name)

		if chunk_arr is None:
			# We don't have it in memory, so check TMP and S3
			(s_chunk_tmp_dir, s_chunk_file_name_tmp) = s3_store.get_chunk_file_tmp_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name)

			(s_chunk_file_dir_s3, s_chunk_file_name_s3) = s3_store.get_chunk_file_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name)

			''' First check if the chunk file exists in TMP,
				Then check if its available in S3, and download it if it does,
				If it doesn't exist, create a new chunk array and initialize it
			'''
			if os.path.exists(s_chunk_file_name_tmp):
				# The file exists locally, open and use it.
				#log.debug("Looking in TMP: %s" % s_chunk_file_name)
				chunk_arr_short = chunk.read_short_chunk_array(s_chunk_file_name_tmp, s_chunk_file_name, config.chunk_size, config.encr_key)
				chunk_arr = self.get_chunk_arr(chunk_arr_short)
			elif s3_store.chunk_file_exists(s_chunk_file_dir_s3, s_chunk_file_name_s3):
				# The chunk array exists on S3, download it locally and use it
				#log.debug("Looking in S3: %s" % s_chunk_file_name)
				s3_store.download_file(s_chunk_file_dir_s3, s_chunk_file_name_s3, s_chunk_file_name_tmp)
				chunk_arr_short = chunk.read_short_chunk_array(s_chunk_file_name_tmp, s_chunk_file_name, config.chunk_size, config.encr_key)
				chunk_arr = self.get_chunk_arr(chunk_arr_short)
			else:
				# The chunk array does not exist, Create a new chunk array
				#log.debug("Creating new chunk array: %s" % s_chunk_file_name)
				chunk_arr = chunk.ChunkArray(s_chunk_file_name, chunk_size, debug_id=4)
				chunk_arr = self.store_zeros_in_chunk_arr(chunk_arr)
			# Add the newly opened chunk array to the list
			chunk_arr_list.add_chunk_arr(chunk_arr)
		else:
			#log.debug("Found open chunk array %s" % s_chunk_file_name)
			pass
				
		return chunk_arr

	def write_chunk_arr(self, fce_pathspec, s_odf_basename, odf_recno, odf_obj, chunk_arr_list, l_array_fce_intervals):
		
		config = self.config

		odf_jsunnoon = fce_pathspec.odf_jsunnoon

		#log.debug("odf_recno=%d" % odf_recno)
		for l_fce_interval in l_array_fce_intervals:
			adjust_ohlc_by = dc.Decimal("0.0")
			adjust_volume_by = 0

			m = -1
			
			if odf_recno % 2 == 0:
				m = 1

			odf_open = odf_obj.get_value(odf_recno, 'ODF_OPEN')
			odf_high = odf_obj.get_value(odf_recno, 'ODF_HIGH')
			odf_low = odf_obj.get_value(odf_recno, 'ODF_LOW')
			odf_close = odf_obj.get_value(odf_recno, 'ODF_CLOSE')
			odf_volume = odf_obj.get_value(odf_recno, 'ODF_VOLUME')

			if config.b_modify_ohlcv_flag:
				adjust_ohlc_by = (config.tick / config.ohlc_divider) * m
				adjust_volume_by = int(odf_volume +  odf_volume * dc.Decimal('0.02'))

			if odf_open + adjust_ohlc_by > 0:
				odf_open = odf_open + adjust_ohlc_by
			if odf_high + adjust_ohlc_by > 0:
				odf_high = odf_high + adjust_ohlc_by
			if odf_low + adjust_ohlc_by > 0:
				odf_low = odf_low + adjust_ohlc_by
			if odf_close + adjust_ohlc_by > 0:
				odf_close = odf_close + adjust_ohlc_by
			if odf_volume + adjust_volume_by > 0:
				odf_volume = odf_volume + adjust_volume_by

			if odf_open < 0  or odf_close < 0:
				log.error(odf_open)
				log.error(odf_high)
				log.error(odf_low)
				log.error(odf_close)
				log.error(odf_volume)
				log.error(adjust_ohlc_by)
				log.error(adjust_volume_by)
				raise ODFException("Invalid values for ODFOPEN & ODF_CLOSE: %f, %f" % (odf_open, odf_close))

			L_no = l_fce_interval[0]
			bar_int = l_fce_interval[1]
			weeks_in_fce = l_fce_interval[2]
			time_shift = l_fce_interval[3]
			four_wk_bar_flag = l_fce_interval[4]

			bar_close_odf_recno = roundup(odf_recno / bar_int) * bar_int
			bar_open_odf_recno = bar_close_odf_recno - bar_int + 1

			fce_jsunnoon = config.first_jsunnoon +int((odf_jsunnoon - config.first_jsunnoon) / ( 7 * weeks_in_fce )) * 7 * weeks_in_fce

			odf_week_no_in_fce = 1 + (odf_jsunnoon - fce_jsunnoon) // 7

			offset = rounddown((10080/bar_int) * (odf_week_no_in_fce - 1))

			fce_recno = rounddown((odf_recno + bar_int - time_shift) / bar_int) + offset

			chunk_no = roundup(fce_recno / config.chunk_size)

			s_chunk_file_name = chunk.make_chunk_file_name(L_no, fce_jsunnoon, chunk_no, s_odf_basename)

			chunk_recno = fce_recno - (chunk_no -1 ) * config.chunk_size

			if chunk_recno <= 0:
				log.info(l_fce_interval)
				log.info(chunk_recno)
				log.info(fce_recno)
				log.info(odf_recno)
				log.info(bar_int)
				log.info(time_shift)
				log.info(offset)
				log.info(config.chunk_size)
				log.info(bar_close_odf_recno)
				log.info(bar_open_odf_recno)
				log.info(fce_jsunnoon)
				log.info(odf_week_no_in_fce)
				raise ODFException("Chunk_recno cannot be negative")


			try:
				chunk_arr = self.get_chunk_arr_ready(fce_pathspec, s_chunk_file_name, chunk_arr_list, L_no, fce_jsunnoon)

				begin_week_of_4_week_bar_orig = (1 + (odf_jsunnoon - config.first_jsunnoon) / 7) /4
				begin_week_of_4_week_bar = rounddown(begin_week_of_4_week_bar_orig)

				four_wk_bar_begins_this_odf_week = False
				if begin_week_of_4_week_bar == begin_week_of_4_week_bar_orig:
					four_wk_bar_begins_this_odf_week = True

				odf_has_no_bar_open = four_wk_bar_begins_this_odf_week * four_wk_bar_flag

				if not odf_has_no_bar_open:
					if odf_recno == bar_open_odf_recno or odf_recno == config.trading_start_recno:
						chunk_arr.set_field(chunk_recno, 'OPEN', float(odf_open))
					if odf_recno < chunk_arr.get_header_field('CHUNK_OPEN_RECNO'):
						chunk_arr.set_header_field('CHUNK_OPEN_RECNO', int(odf_recno))

				if not four_wk_bar_flag:
					if odf_recno == bar_close_odf_recno or odf_recno == config.highest_recno:
						chunk_arr.set_field(chunk_recno, 'CLOSE', float(odf_close))
					if odf_recno > chunk_arr.get_header_field('CHUNK_CLOSE_RECNO'):
						chunk_arr.set_header_field('CHUNK_CLOSE_RECNO', int(odf_recno))
				else:
					chunk_arr.set_field(chunk_recno, 'CLOSE', float(odf_close))
					chunk_arr.set_header_field('CHUNK_CLOSE_RECNO', int(odf_recno))


				odf_recno_high = odf_obj.get_value(odf_recno, 'ODF_HIGH')
				odf_recno_low = odf_obj.get_value(odf_recno, 'ODF_LOW')

				#log.debug("odf_recno HIGH: %f, LOW: %f" % (odf_recno_high, odf_recno_low))
				chunk_recno_high = chunk_arr.get_field(chunk_recno, 'HIGH')
				chunk_recno_low = chunk_arr.get_field(chunk_recno, 'LOW')
				chunk_recno_volume = chunk_arr.get_field(chunk_recno, 'VOLUME')

				if odf_recno_high > chunk_recno_high:
					chunk_arr.set_field(chunk_recno, 'HIGH', float(odf_high))

				if odf_recno_low < chunk_recno_low:
					chunk_arr.set_field(chunk_recno, 'LOW', float(odf_low))

				chunk_arr.set_field(chunk_recno, 'VOLUME', float(chunk_recno_volume + float(odf_volume)))
			except:
				log.exception("Error processing chunk_recno=%d" % chunk_recno)
				raise

		return chunk_arr_list

	def process_fce(self, fce_pathspec, s_odf_dd, s_odf_basename, odf_obj, s_odf_s3, l_array_fce_intervals):
		odf_jsunnoon = fce_pathspec.odf_jsunnoon
		config = self.config
		s3_store = self.s3_store
		dd_store = self.dd_store

		fce_obj = None

		if not s3_store.fce_exists(fce_pathspec, fce_pathspec.s_fce_header_file_name):
			fce_obj = fce.FCE(config)
			log.debug("Saving FCE to S3: %s..." % fce_pathspec.s_fce_header_file_name)
			s3_store.save_fce(fce_pathspec, fce_pathspec.s_fce_header_file_name, fce_obj, config.encr_key)

		current_jsunnoon = self.get_current_jsunnoon()

		if config.last_fced_recno == config.highest_recno and not odf_jsunnoon == current_jsunnoon:
			# Load the entire ODF from DD and copy it to S3
			log.debug("Saving ODF to S3 at %s..." % s_odf_s3)
			s3_store.save_odf(s_odf_s3, odf_obj)
			return
		
		if config.last_fced_recno == config.highest_recno:
			log.debug("Already processed this ODF. Skipping...")
			return
		
		
		chunk_arr_list = chunk.ChunkArrayList()

		log.debug("Generating chunk files... %d:%d" % (int(config.last_fced_recno), int(config.highest_recno)))
		debug_freq = 500
		for odf_recno in odf_obj.get_recnos_within_limits(config):
			if odf_recno % debug_freq == 0:
				log.debug("write_chunk_arr:odf_recno=%d" % odf_recno)
			chunk_arr_list = self.write_chunk_arr(fce_pathspec, 
													s_odf_basename, 
													odf_recno, 
													odf_obj, 
													chunk_arr_list, 
													l_array_fce_intervals)
		"""
		odf_recno = config.last_fced_recno
		debug_freq = 100
		while True:
			odf_recno += 1
			if odf_obj.is_recno_out_of_limits(odf_recno, config.trading_start_recno, config.trading_recs_perday):
				if odf_recno > config.highest_recno:
					break
				continue
			if odf_recno % debug_freq == 0:
				log.debug("write_chunk_arr:odf_recno=%d" % odf_recno)
			chunk_arr_list = self.write_chunk_arr(fce_pathspec, s_odf_basename, odf_recno, odf_obj, chunk_arr_list, l_array_fce_intervals)
			
			if odf_recno >= config.highest_recno:
				break
		"""
		log.debug("Preparing ShortChunk List from %d chunk arrays..." % chunk_arr_list.length())

		chunk_arr_short_list = self.get_chunk_arr_short_list(chunk_arr_list)

		log.debug("Writing %d chunk files..." % chunk_arr_short_list.length())
		
		self.write_chunk_files(fce_pathspec, chunk_arr_short_list)

		odf_obj.set_header_value(config.last_fced_recno_storloc, int(config.highest_recno))
		dd_store.save_odf(s_odf_dd, s_odf_basename, odf_obj)

	def load_public_from_odf(self, odf_obj, config):
		# Get the common ODF headers
		odf_gmt_offset = odf_obj.get_header_value(config.gmt_offset_storloc)
		odf_trading_start_recno = odf_obj.get_header_value(config.trading_start_recno_storloc)
		odf_trading_recs_perday = odf_obj.get_header_value(config.trading_recs_perday_storloc)
		odf_idf_currency = odf_obj.get_header_value(config.idf_currency_storloc)
		odf_idf_currency_max_decimals = odf_obj.get_header_value(config.idf_currency_max_decimals_storloc)
		odf_split_factor = odf_obj.get_header_value(config.split_factor_storloc)
		odf_currency_value_of_point = odf_obj.get_header_value(config.currency_value_of_point_storloc)
	
		# Get the 'specific' odf headers, which may be missing
		odf_tick = odf_obj.get_header_value(config.tick_storloc)
		odf_ohlc_divider = odf_obj.get_header_value(config.ohlc_divider_storloc)
		odf_last_fced_recno = odf_obj.get_header_value(config.last_fced_recno_storloc)
		odf_highest_recno = odf_obj.get_header_value(config.highest_recno_storloc)
		odf_highest_recno_close = odf_obj.get_header_value(config.highest_recno_close_storloc)
		odf_prev_highest_recno_close = odf_obj.get_header_value(config.prev_highest_recno_close_storloc)
	
		# Save this ODF's common headers to public values
		config.gmt_offset = odf_gmt_offset
		config.trading_start_recno = odf_trading_start_recno
		config.trading_recs_perday = odf_trading_recs_perday
		config.idf_currency = odf_idf_currency
		config.idf_currency_max_decimals = odf_idf_currency_max_decimals
		config.split_factor = odf_split_factor
		config.currency_value_of_point = odf_currency_value_of_point

		# Save the ODf-specific header to public values
		config.tick = odf_tick
		config.ohlc_divider = odf_ohlc_divider
		config.last_fced_recno = odf_last_fced_recno
		config.highest_recno = odf_highest_recno
		config.highest_recno_close = odf_highest_recno_close
		config.prev_highest_recno_close = odf_prev_highest_recno_close


	def odf2fce(self):
		''' Processes ODFs on local storage or S3 and updates FCEs on local storage or DD
		'''
		log.info(dt.datetime.now())
		log.info("-----CYCLE START-----")
		
		''' Get a handle to the configuration
		'''
		config = self.config

		''' Get the DD & S3 store objects
		'''
		self.dd_store = dd.get_dd_store(config)
		self.s3_store = s3.get_s3_store(config)

		
		''' Get current working directory
		'''		
		s_app_dir = self.get_working_dir()

		''' Get the number of exchanges we have to process
		'''
		ls_exchanges = self.dd_store.list_exchanges()

		for s_exchange in ls_exchanges:
			
			s_exchange_basename = self.dd_store.get_exchange_basename(s_exchange)

			''' If we cannot process data, skip
			'''
			if not config.b_process_data:
				continue

			''' Get the number of odf's in i-th exchange
			'''
			ls_odf_names = self.dd_store.list_odfs(s_exchange)

			for s_odf_dd in ls_odf_names:

				s_odf_basename = self.dd_store.get_odf_basename(s_odf_dd)

				''' Extract symbol name from ODF basename (string before first '_')
				'''
				s_symbol = self.dd_store.get_odf_symbol(s_odf_basename)

				s_odf_s3 = self.s3_store.get_odf_path(s_exchange_basename, s_odf_basename)

				''' Make FIFO name for this ODF from the ODF's basename
				'''
				s_fifo_dd = self.dd_store.get_fifo_path(s_exchange_basename, s_odf_basename)
				s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

				''' Get path to the FCE for this ODF on S3
				''' 
				
				fce_pathspec = self.s3_store.get_fce_pathspec(s_app_dir,
																s_exchange_basename,
																s_symbol,
																s_odf_basename)

				''' Print the ODF path on DD if required
				'''
				if self.b_show:
					if config.b_test_mode:
						log.info(s_odf_dd)
					else:
						log.info(':'.join([s_exchange, s_odf_dd]))

				odf_obj = self.dd_store.open_odf(s_exchange_basename, s_odf_dd)

				'''
				Load public variables from odf_obj
				'''
				
				self.load_public_from_odf(odf_obj, config)
				
				
				if config.b_test_mode and self.b_print_mode:
					self.print_fifo_local(s_exchange_basename, s_odf_dd, s_fifo_dd)
					continue

				''' Refresh the FIFO from ODF data
				'''
				fifo_obj = self.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

				self.fill_missing_odf_header_records(odf_obj, s_odf_dd, s_odf_basename, fifo_obj, fce_pathspec)

				self.fill_missing_odf_records(odf_obj)

				l_array_fce_intervals = self.fill_fce_intervals_array()

				self.process_fce(fce_pathspec, s_odf_dd, s_odf_basename, odf_obj, s_odf_s3, l_array_fce_intervals)


		log.info(dt.datetime.now())
		log.info("-----CYCLE DONE-----")

	def execute(self):
		""" Execute the commands passed on the command line.
		"""

		if self.b_ask:

			self.prompt_interactive()

		self.odf2fce()


	def initialize_logging(self, s_name):
		""" Configure logging handlers, levels & formatting.
		@param s_logfile: Path to log file
		"""

		s_cwd = self.get_working_dir()

		s_logdir = os.sep.join([s_cwd, "logs"])
		# Make the log directory
		if not os.path.exists(s_logdir):
			os.makedirs(s_logdir)

		s_logfile = '.'.join([s_name, "log"])

		# Set the log file path
		s_logfile = os.sep.join([s_logdir, s_logfile])

		# Get the root logger
		logger = logging.getLogger()
		logger.setLevel(logging.ERROR)


		s_logdir = os.path.dirname(s_logfile)
		if not os.path.exists(s_logdir):
			os.makedirs(s_logdir)

		# Create log file handler
		fh = logging.handlers.RotatingFileHandler(s_logfile, 
												maxBytes=64*1024*1024, 
												backupCount=25)
		fh.setLevel(logging.DEBUG)
		
		# Create console handler
		ch = logging.StreamHandler()
		ch.setLevel(logging.INFO)

		# Set formats for file & console handlers
		log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

		con_formatter = logging.Formatter('********** %(levelname)s - %(message)s')

		fh.setFormatter(log_formatter)
		ch.setFormatter(con_formatter)


		# Close existing log handlers. This is to handle
		# the case where this is called repeatedly by the unit tester.
		l_handlers = list(logger.handlers)

		for handler in l_handlers:
			if hasattr(handler, "close"):
				handler.close()

		logger.handlers = []

		logger.addHandler(fh)
		logger.addHandler(ch)

		ls_modules = ['binary', 'fifo', 'odf', 'fce', 'odfproc', 'odfexcept',
						'dd', 'odf2fce', 's3', 'config', 'chunk', '__main__']

		# Set debugging on for local modules.
		# This ensures we don't get Boto & other library debug in our logs
		for s_module in ls_modules:
			mod_log = logging.getLogger(s_module)
			mod_log.setLevel(logging.DEBUG)


	def main(self):
		""" Entry point for text_odf_export application.
		"""
		s_name = re.sub(r'\.py', '', os.path.basename(__file__))

		
		self.initialize_logging(s_name)

		self.d_settings = {}

		try:
			self.config = config.Config()

			self.config.read_settings("settings.txt")

		except:
			log.exception("Error in settings:")
			log.fatal("Error reading settings.txt.")
			sys.exit(-1)

		try:
			args = self.arg_parse()

			self.set_args(args)
			
			log.info("%s: Starting up." % s_name)
			
			self.execute()	
		except KeyboardInterrupt:
			log.error("%s: Terminated prematurely." % s_name)
			sys.exit(-1)
		except SystemExit:
			log.info("%s: Aborted." % s_name)
			sys.exit(-1)
		except:
			log.exception("%s: Unhandled exception. Terminating." % s_name)
			sys.exit(-1)

		log.info("%s: Completed task." % s_name)

def test_settings():
	app = Odf2Fce()
	app.config = config.Config()
	app.config.read_settings("settings.txt")

def test_ls_exchanges():
	app = Odf2Fce()
	app.config = config.Config()
	app.config.read_settings("settings.txt")
	return app.list_exchanges()

def test_ls_odfs(s_exchange):
	app = Odf2Fce()
	app.config = config.Config()
	app.config.read_settings("settings.txt")
	return app.list_odfs(s_exchange)

if __name__ == "__main__":
	app = Odf2Fce()
	app.main()

