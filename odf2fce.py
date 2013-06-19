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
	return math.ceil(n)

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
		s_working_dir = os.path.dirname(__file__)
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
				dd_store.save_fifo(fifo_obj)
			else:
				log.debug("FIFO is too recent (<25 days old). Skipping Update...")
				fifo_obj = dd_store.open_fifo(s_fifo_dd, s_fifo_basename)
		else:

			l_fifo_arr = odf_obj.get_fifo_arr(config.fifo_count,
												config.trading_start_recno)

			fifo_obj = FIFO()
			# Need to optimize to reduce writes. only updates entries should be saved back.
			fifo_obj.read_list(l_fifo_arr)
			dd_store.save_fifo(fifo_obj)

		return fifo_obj

	
	def print_fifo_local(self, odf_obj, s_fifo_dd):

		fifo_obj = FIFO()
		fp_fifo_dd = open(s_fifo_dd, "rb")
		fifo_obj.read_bin_stream(fp_fifo_dd)
		fp_fifo_dd.close()

		buf = str(fifo_obj)

		log.debug("FIFO %s TEXT:\n%s\n" % (s_fifo_dd, buf))

	def fill_missing_odf_header_records(self, odf_obj, s_odf_dd, fifo_obj, fce_pathspec):

		config = self.config
		dd_store = self.dd_store
		s3_store = self.s3_store

		odf_tick = odf_obj.get_header_value(config.tick_storloc)
		odf_ohlc_divider = odf_obj.get_header_value(config.ohlc_divider_storloc)
		odf_last_fced_recno = odf_obj.get_header_value(config.last_fced_recno_storloc)
		odf_highest_recno = odf_obj.get_header_value(config.highest_recno_storloc)
		odf_highest_recno_close = odf_obj.get_header_value(config.highest_recno_close_storloc)
		odf_prev_highest_recno_close = odf_obj.get_header_value(config.prev_highest_recno_close_storloc)

		# Check if the 'missing headers' are missing or have been added previously.
		if odf_tick > 0 and odf_ohlc_divider > 0 and odf_last_fced_recno > 0 and \
			odf_highest_recno > 0 and odf_highest_recno_close > 0 and \
			odf_prev_highest_recno_close > 0:
			return None

		# Get the missing header values for this ODF
		# Get the common ODF headers
		odf_gmt_offset = odf_obj.get_header_value(config.gmt_offset_storloc)
		odf_trading_start_recno = odf_obj.get_header_value(config.trading_start_recno_storloc)
		odf_trading_recs_perday = odf_obj.get_header_value(config.trading_recs_perday_storloc)
		odf_idf_currency = odf_obj.get_header_value(config.idf_currency_storloc)
		odf_idf_currency_max_decimals = odf_obj.get_header_value(config.idf_currency_max_decimals_storloc)
		odf_split_factor = odf_obj.get_header_value(config.split_factor_storloc)
		odf_currency_value_of_point = odf_obj.get_header_value(config.currency_value_of_point_storloc)

		# Save this ODF's headers to public values
		config.gmt_offset = odf_gmt_offset
		config.trading_start_recno = odf_trading_start_recno
		config.trading_recs_perday = odf_trading_recs_perday
		config.idf_currency = odf_idf_currency
		config.idf_currency_max_decimals = odf_idf_currency_max_decimals
		config.split_factor = odf_split_factor
		config.currency_value_of_point = odf_currency_value_of_point

		odf_tick = fifo_obj.get_tick()
		odf_ohlc_divider = fifo_obj.get_ohlc_divider()
		odf_last_fced_recno = 0
		odf_highest_recno = odf_obj.find_highest_recno(config.trading_start_recno, 
														config.trading_recs_perday)
		odf_highest_recno_close = odf_obj.get_field(odf_highest_recno, 'ODF_CLOSE')

		prev_fce_obj = s3_store.fce_open(fce_pathspec.s_prev_fce_header_file_name)

		odf_prev_highest_recno_close = odf_obj.find_prev_highest_recno_close(prev_fce_obj)

		if odf_prev_highest_recno_close == 0:
			odf_prev_highest_recno_close = odf_obj.find_first_non_zero_open(config.trading_start_recno)

		# Fill missing headers in the ODF
		odf_obj.add_missing_header(config.tick_storloc, odf_tick)
		odf_obj.add_missing_header(config.ohlc_divider_storloc, odf_ohlc_divider)
		odf_obj.add_missing_header(config.last_fced_recno_storloc, odf_last_fced_recno)
		odf_obj.add_missing_header(config.highest_recno_storloc, odf_highest_recno)
		odf_obj.add_missing_header(config.highest_recno_close_storloc, odf_highest_ercno_close)
		odf_obj.add_missing_header(config.prev_highest_recno_close_storloc, odf_prev_highest_recno_close_storloc)

		# Save the ODF
		dd_store.save_odf(s_odf_dd, s_odf_basename, odf_obj)

		config.tick = odf_tick_recno
		config.ohlc_divider = odf_ohlc_divider
		config.last_fced_recno = odf_last_fced_recno
		config.highest_recno = odf_highest_recno
		config.highest_recno_close = odf_highest_recno_close
		config.prev_highest_recno_close = odf_prev_highest_recno_close

	def fill_missing_odf_records(odf_obj):
		config = self.config

		r = config.last_fced_recno

		dc_odf_close = odf_obj.get_field(r, 'ODF_CLOSE')

		if r == 0:
			r = config.trading_start_recno - 1
			c = config.prev_highest_recno_close

		while True:
			r = r + 1
			if r > config.highest_recno:
				return True
			if odf_obj.is_recno_out_of_limits(r):
				continue

			dc_odf_open = odf_obj.get_field(r, 'ODF_OPEN')
			dc_odf_high = odf_obj.get_field(r, 'ODF_HIGH')
			dc_odf_low = odf_obj.get_field(r, 'ODF_LOW')
			dc_odf_close = odf_obj.get_field(r, 'ODF_CLOSE')

			if dc_odf_open == 0 or dc_odf_high == 0 or \
				dc_odf_low == 0 or dc_odf_close == 0:
				''' Fill the missing record
				''' 
				odf_obj.add_missing_record(r, 
											dc_odf_open, 
											dc_odf_high, 
											dc_odf_low, 
											dc_odf_close, 
											dc_odf_volume)
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
		
		today_noon = dt.datetime(today.year, today.month, today,day, 12, 0)

		sunday_last_noon = today_noon - timedelta(days=day.weekday()) - timedelta(days=1)

		return self.get_julian_day_number(sunday_last_noon)

	def write_chunk_files(self, chunk_arr_short_list):
		
		for i in range(chunk_arr_short_list.length()):
			chunk_arr_short = chunk_arr_short_list.get_chunk_arr_at(i)
			s_chunk_arr_short_name = chunk_arr_short.get_name()
			s_chunk_file_name = '.'.join(s_chunk_arr_short_name, 'fce')

			(L_no, fce_jsunnoon, chunk_no) = chunk.get_components_from_chunk_name(s_chunk_arr_short_name)

			s_chunk_file_name_s3 = os.sep.join([s_fce_s3, str(L_no), str(fce_jsunnoon), s_chunk_file_name])

			s_chunk_file_name_tmp = os.sep.join([s_fce_tmp, str(L_no), str(fce_jsunnoon), s_chunk_file_name])

			chunk_arr_short.save_encrypted_to(s_chunk_file_name_tmp)

			s_chunk_csv_name_tmp = re.sub(r'\.fce$', '\.csv', s_chunk_file_name_tmp)

			if self.config.b_do_csv_chunk:
				chunk_arr_short.save_csv_to(s_chunk_csv_name_tmp)

			s3.move_up(s_chunk_csv_name_tmp, s_chunk_file_name_s3)

	def find_first_non_zero_chunk_open(self, chunk_arr):
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


	def find_last_non_zero_chunk_close(self, chunk_arr):
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
		
		(first_nonzero_chunk_recno, first_nonzero_chunk_open) = self.find_first_non_zero_chunk_open(chunk_arr)

		m = first_nonzero_chunk_recno

		# First try back-fill, then fall back to forward-fill
		while True:
			m = m - 1		

			if m < 1:
				(last_nonzero_chunk_recno, last_nonzero_chunk_close) = self.find_last_nonzero_chunk_close(chunk_arr)
				m = last_nonzero_chunk_recno
				while True:
					m = m + 1
					if m > self.chunk_size:
						return chunk_arr
					else:
						chunk_arr.set_field(m, 'OPEN', first_nonzero_chunk_close)
						chunk_arr.set_field(m, 'HIGH', first_nonzero_chunk_close)
						chunk_arr.set_field(m, 'LOW', first_nonzero_chunk_close)
						chunk_arr.set_field(m, 'CLOSE', first_nonzero_chunk_close)
			else:
				chunk_arr.set_field(m, 'OPEN', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'HIGH', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'LOW', first_nonzero_chunk_open)
				chunk_arr.set_field(m, 'CLOSE', first_nonzero_chunk_open)


	def get_chunk_arr_short_list(self, chunk_arr_list):
		
		highest_volume = 0
		lowest_low = 0
		chunk_arr_short_list = ChunkArrayList()
		for i in range(chunk_arr_list.length()):
			chunk_arr = chunk_arr_list.get_chunk_arr_at(i)

			highest_volume = chunk_arr.highest_volume()
			lowest_low = chunk_arr.lowest_low()

			chunk_arr = self.fill_empty_chunk_records(chunk_arr)

			chunk_arr_short = self.get_chunk_arr_short(chunk_arr)

			chunk_arr_short_list.add_chunk_arr(chunk_arr_short)

		return chunk_arr_short_list

	def get_chunk_arr_short(self, chunk_arr):

		chunk_arr_short = chunk.ChunkArray(chunk_arr.get_name(), self.config.chunk_size)

		for i in range(self.config.chunk_size):
			vol_val = rounddown(chunk_arr.get_field(i+1, 'VOLUME') / self.config.volume_tick)
			open_val = rounddown(chunk_arr.get_field(i+1, 'OPEN') / self.config.tick)
			high_val = rounddown(chunk_arr.get_field(i+1, 'HIGH') / self.config.tick)
			low_val = rounddown(chunk_arr.get_field(i+1, 'LOW') / self.config.tick)
			close_val = rounddown(chunk_arr.get_field(i+1, 'CLOSE') / self.config.tick)

		d_chunk_header = {
			'LOWEST_LOW' : chunk_arr.get_header_field('LOWEST_LOW'),
			'VOLUME_TICK' : chunk_arr.get_header_field('VOLUME_TICK'),
			'CHUNK_OPEN_RECNO' : chunk_arr.get_header_field('CHUNK_OPEN_RECNO'),
			'CHUNK_CLOSE_RECNO' : chunk_arr.get_header_field('CHUNK_CLOSE_RECNO'),
		}

		chunk_arr_short.set_header(d_header)

		return chunk_arr_short


	def get_chunk_arr(self, chunk_arr_short):
		
		chunk_arr = ChunkArray(chunk_arr_short.get_name(), self.config.chunk_size)
		for i in range(self.config.chunk_size):
			vol_val = chunk_arr_short.get_field(i+1, 'VOLUME') * self.config.volume_tick
			open_val = chunk_arr_short.get_field(i+1, 'OPEN') * self.config.tick
			high_val = chunk_arr_short.get_field(i+1, 'HIGH') * self.config.tick
			low_val = chunk_arr_short.get_field(i+1, 'LOW') * self.config.tick
			close_val = chunk_arr_short.get_field(i+1, 'CLOSE') * self.config.tick

			chunk_arr.set_field(i+1, 'VOLUME', vol_val)
			chunk_arr.set_field(i+1, 'OPEN', open_val)
			chunk_arr.set_field(i+1, 'HIGH', high_val)
			chunk_arr.set_field(i+1, 'LOW', low_val)
			chunk_arr.set_field(i+1, 'CLOSE', close_val)

		d_chunk_header = {
			'LOWEST_LOW' : chunk_arr_short.get_header_field('LOWEST_LOW') / self.config.ohlc_divider,
			'VOLUME_TICK' : chunk_arr_short.get_header_field('VOLUME_TICK'),
			'CHUNK_OPEN_RECNO' : chunk_arr_short.get_header_field('CHUNK_OPEN_RECNO'),
			'CHUNK_CLOSE_RECNO' : chunk_arr_short.get_header_field('CHUNK_CLOSE_RECNO'),
		}

		chunk_arr.set_header(d_header)

		return chunk_arr

	def store_zeros_in_chunk_arr(self, chunk_arr):
		#chunk_arr = ChunkArray(chunk_arr.get_name(), self.config.chunk_size)

		for i in range(self.config.chunk_size):
			chunk_arr.set_field(i+1, 'OPEN', 0)
			chunk_arr.set_field(i+1, 'HIGH', 0)
			chunk_arr.set_field(i+1, 'LOW', 0)
			chunk_arr.set_field(i+1, 'CLOSE', 0)
			chunk_arr.set_field(i+1, 'VOLUME', 0)
		
		d_chunk_header = {
			'LOWEST_LOW' : 0,
			'VOLUME_TICK' : 0,
			'CHUNK_OPEN_RECNO' : self.config.highest_recno,
			'CHUNK_CLOSE_RECNO' : self.config.trading_start_recno,
		}

		chunk_arr.set_header(d_chunk_header)

		return chunk_arr

	def get_chunk_arr_ready(self, fce_pathspec, s_chunk_file_name, chunk_arr_list, L_no, fce_jsunnoon):
		
		s3_store = self.s3_store

		chunk_size = self.config.chunk_size
		chunk_arr = chunk_arr_list.get_chunk_arr(s_chunk_file_name)

		if chunk_arr is None:
			chunk_arr = chunk.ChunkArray(s_chunk_file_name, chunk_size)
			chunk_arr_list.add_chunk_arr(chunk_arr)			

		s_chunk_file_name_tmp = s3_store.get_chunk_file_tmp_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name)

		(s_chunk_file_dir_s3, s_chunk_file_name_s3) = s3_store.get_chunk_file_path(fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name)

		# Use temp file if available, else fall back to S3
		if os.path.exists(s_chunk_file_name_tmp):
			chunk_arr_short = chunk.read_short_chunk_array(s_chunk_file_name_tmp)
			chunk_arr = self.get_chunk_arr(chunk_arr_short)
			return chunk_arr

		if s3_store.chunk_file_exists(s_chunk_file_dir_s3, s_chunk_file_name_s3):
			# copy file to tmp
			s3_store.download_chunk_file(s_chunk_file_dir_s3, s_chunk_file_name_s3, s_chunk_file_name_tmp)
			chunk_arr_short = chunk.read_short_chunk_array(s_chunk_file_name_tmp)
			chunk_arr = self.get_chunk_arr(chunk_arr_short)
			return chunk_arr			

		chunk_arr = self.store_zeros_in_chunk_arr(chunk_arr)

		return chunk_arr

	def write_chunk_arr(self, fce_pathspec, odf_basename, odf_recno, odf_obj, chunk_arr_list, l_array_fce_intevals):
		
		config = self.config

		odf_jsunnoon = fce_pathspec.odf_jusunnoon
		chunk_header_recno = config.chunk_size + 1

		for l_fce_interval in l_array_fce_intervals:
			adjust_ohlc_by = 0
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
				adjust_ohlc_by = config.tick / config.ohlc_divider
				adjust_volume_by = int(odf_volume +  odf_volume * 0.02)

			odf_open = odf_open + adjust_ohlc_by
			odf_high = odf_high + adjust_ohlc_by
			odf_low = odf_low + adjust_ohlc_by
			odf_close = odf_close + adjust_ohlc_by
			odf_volume = odf_volume + adjust_volume_by

			L_no = l_fce_interval[0]
			bar_int = l_fce_interval[1]
			weeks_in_fce = l_fce_interval[2]
			time_shift = l_fce_interval[3]
			four_wk_bar_flag = l_fce_interval[4]

			bar_close_odf_recno = roundup(odf_recno / bar_int) * bar_int
			bar_open_odf_recno = bar_close_odf_recno - bar_int + 1

			fce_jsunnoon = config.first_jsunnoon +int((odf_jsunnoon - config.first_jsunnoon) / ( 7 * weeks_in_fce ))* 7 * weeks_in_fce

			odf_week_no_in_fce = 1 + (odf_jsunnoon - fce_jsunnoon) / 7

			fce_recno = rounddown((odf_recno + bar_int -time_shift) / bar_int) + gmt_offset
			chunk_no = roundup(fce_recno / config.chunk_size)

			s_chunk_file_name = chunk.make_chunk_file_name(L_no, fce_jsunnoon, chunk_no, s_odf_basename)

			chunk_recno = fce_recno - (chunk_no -1 ) * config.chunk_size

			chunk_arr = self.get_chunk_arr_ready(fce_pathspec, s_chunk_file_name, chunk_arr_list, L_no, fce_jsunnoon)

			begin_week_of_4_week_bar_orig = (1 + (odf_jsunnoon - config.first_jsunnoon) / 7) /4
			begin_week_of_4_week_bar = rounddown(begin_week_of_4_week_bar_orig)

			if begin_week_of_4_week_bar == begin_week_of_4_week_bar_orig:
				four_wk_bar_begins_this_odf_week = True

			odf_has_no_bar_open = four_wk_bar_begins_this_odf_week * four_wk_bar_flag

			if not odf_has_no_bar_open:
				if odf_recno == bar_open_odf_recno or odf_recno == config.trading_start_recno:
					chunk_arr.set_field(chunk_recno, 'OPEN', odf_open)
				if odf_recno < chunk_open_recno:
					chunk_open_recno = odf_recno

			if not four_wk_bar_flag:
				if odf_recno == bar_close_odf_recno or odf_recno == config.highest_recno:
					chunk_arr.set_field(chunk_recno, 'CLOSE', odf_close)
				if odf_recno > chunk_close_recno:
					chunk_close_recno = odf_recno
			else:
				chunk_arr.set_field(chunk_recno, 'CLOSE', odf_close)
				chunk_close_recno = odf_recno


			odf_recno_high = odf_obj.get_value(odf_recno, 'ODF_HIGH')
			odf_recno_low = odf_obj.get_value(odf_recno, 'ODF_LOW')

			chunk_recno_high = chunk_arr.get_field(chunk_recno, 'HIGH')
			chunk_recno_low = chunk_arr.get_field(chunk_recno, 'LOW')
			chunk_recno_volume = chunk_arr.get_field(chunk_recno, 'VOLUME')

			if odf_recno_high > chunk_recno_high:
				chunk_arr.set_field(chunk_recno, 'HIGH', odf_high)

			if odf_recno_low < chunk_recno_low:
				chunk_arr.set_field(chunk_recno, 'LOW', odf_low)

			chunk_arr.set_field(chunk_recno, 'VOLUME', chunk_recno_volume + chunk_recno_volume)

		return chunk_arr_list

	def process_fce(self, fce_pathspec, odf_obj, s_odf_s3, l_array_fce_intervals):
		odf_jsunnoon = fce_pathspec.odf_jsunnoon
		config = self.config
		s3_store = self.s3_store
		
		fce_obj = None

		if not s3_store.fce_exists(fce_pathspec, fce_pathspec.s_fce_header_filename):
			fce_obj = fce.FCE(config)
			s3_store.save_fce(fce_pathspec, fce_pathspec.s_fce_header_filename, fce_obj)

		current_jsunnoon = self.get_current_jsunnoon()

		if config.last_fced_recno == config.highest_recno and not odf_jsunnoon == current_jsunnoon:
			# Load the entire ODF from DD and copy it to S3
			s3_store.save_odf(s_odf_s3, odf_obj)
			return
		
		if config.last_fced_recno == self.highest_recno:
			return
		
		odf_recno = config.last_fced_recno
		chunk_arr_list = chunk.ChunkArrayList()

		while True:
			odf_recno += 1
			if odf_obj.is_recno_out_of_limits(odf_recno):
				continue

			chunk_arr_list = self.write_chunk_arr(odf_recno, odf_obj, chunk_arr_list, l_array_fce_intevals)

			if odf_recno >= config.highest_recno:
				break

		chunk_arr_short_list - self.get_chunk_arr_short_list(chunk_arr_list)

		self.write_chunk_files(chunk_arr_short_list)

		odf_obj.set_header_value('LAST_FCED_RECNO', config.highest_recno)


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

				s_odf_s3 = self.s3_store.get_odf_path(s_exchange_basename, s_symbol, s_odf_basename)

				''' Make FIFO name for this ODF from the ODF's basename
				'''
				s_fifo_dd = self.dd_store.get_fifo_path(s_exchange, s_odf_basename)
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

				odf_obj = self.dd_store.open_odf(s_exchange, s_odf_dd)

				if config.b_test_mode and self.b_print_mode:
					self.print_fifo_local(s_exchange_basename, s_odf_dd, s_fifo_dd)
					continue

				''' Refresh the FIFO from ODF data
				'''
				fifo_obj = self.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

				self.fill_missing_odf_header_records(odf_obj, s_odf_dd, fifo_obj, fce_pathspec)

				self.fill_missing_odf_records(odf_obj)

				l_array_fce_intervals = self.fill_fce_intervals_array()

				self.process_fce(fce_pathspec, odf_obj, s_odf_s3, l_array_fce_intervals)


		log.info(dt.datetime.now())
		log.info("-----CYCLE DONE-----")

	def execute(self):
		""" Execute the commands passed on the command line.
		"""

		if self.b_ask:
			self.prompt_interactive()

		self.odf2fce()


	def initialize_logging(self, s_logfile):
		""" Configure logging handlers, levels & formatting.
		@param s_logfile: Path to log file
		"""
		# Get the root logger
		logger = logging.getLogger()
		logger.setLevel(logging.ERROR)

		# Create log file handler
		fh = logging.handlers.RotatingFileHandler(s_logfile, 
												maxBytes=8*1024*1024, 
												backupCount=5)
		fh.setLevel(logging.DEBUG)
		
		# Create console handler
		ch = logging.StreamHandler()
		ch.setLevel(logging.INFO)

		# Set formats for file & console handlers
		log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

		con_formatter = logging.Formatter('********** %(levelname)s - %(message)s')

		fh.setFormatter(log_formatter)
		ch.setFormatter(con_formatter)

		logger.addHandler(fh)
		logger.addHandler(ch)

		ls_modules = ['binary', 'fifo', 'odf', 'odfproc', 'odfexcept', 'dd', '__main__']

		# Set debugging on for local modules.
		# This ensures we don't get Boto & other library debug in our logs
		for s_module in ls_modules:
			mod_log = logging.getLogger(s_module)
			mod_log.setLevel(logging.DEBUG)

	def main(self):
		""" Entry point for text_odf_export application.
		"""
		s_name = re.sub('\.py', '', os.path.basename(__file__))

		# Make the log directory
		if not os.path.exists("logs"):
			os.mkdir("logs")

		# Set the log file path
		s_logfile = os.sep.join(["logs", "text_odf_export.log"])

		self.initialize_logging(s_logfile)

		self.d_settings = {}

		try:
			self.config = config.Config()

			self.config.read_settings("settings.txt")

		except:
			log.exception()
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
	app.config = Config()
	app.config.read_settings("settings.txt")

def test_ls_exchanges():
	app = Odf2Fce()
	app.config = Config()
	app.config.read_settings("settings.txt")
	return app.list_exchanges()

def test_ls_odfs(s_exchange):
	app = Odf2Fce()
	app.config = Config()
	app.config.read_settings("settings.txt")
	return app.list_odfs(s_exchange)

if __name__ == "__main__":
	app = Odf2Fce()
	app.main()

