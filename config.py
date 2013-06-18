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
import decimal as dc

import logging
log = logging.getLogger(__name__)

class Config(object):
	
	def __init__(self):
		self.ddstore=None

	def read_settings(self, s_settings_file):
		""" Load settings from the settings file
		@param s_settings_file: Path to the settings file.
		"""
		fp_settings = open(s_settings_file, "r")

		s_settings = fp_settings.read()

		fp_settings.close()

		d_settings = eval(s_settings)

		self.refresh_settings(d_settings)
		
	def refresh_settings(self, d_settings):

		self.d_settings = d_settings

		try:
			self.s_local_dd_data_root = d_settings["LD_DD_DATA_ROOT"]
			self.s_local_s3_data_root = d_settings["LD_S3_DATA_ROOT"]
			self.s_dd_data_root = d_settings["LD_DD_DATA_ROOT"]
			self.s_s3_data_root = d_settings["LD_S3_DATA_ROOT"]
				

			self.trading_start_recno = int(d_settings["TRADING_START_RECNO"])
			self.b_process_data = bool(d_settings["PROCESS_DATA"])

			self.s_dd_region = d_settings["DDREGION"]
			self.dd_read_units = int(d_settings["DDREADUNITS"])
			self.dd_write_units_opt = int(d_settings["DDWRITEUNITS_OPT"])
			self.dd_write_units = int(d_settings["DDWRITEUNITS"])
			self.s_dd_access_key = d_settings["DDACCESSKEY"]
			self.s_dd_secret_access_key = d_settings["DDSECRETACCESSKEY"]

			self.s_s3_access_key = d_settings["S3ACCESSKEY"]
			self.s_s3_secret_access_key = d_settings["S3SECRETACCESSKEY"]
			self.s_s3_bucket= d_settings["S3BUCKET"]

			self.s_dd_data_root = d_settings["DD_DATA_ROOT"]
			self.s_s3_data_root = d_settings["S3_DATA_ROOT"]
			self.s_ld_dd_data_root = d_settings["LD_DD_DATA_ROOT"]
			self.s_ld_s3_data_root = d_settings["LD_S3_DATA_ROOT"]
			self.s_ld_data_root = d_settings["LD_DATA_ROOT"]
			self.b_test_mode = bool(d_settings["TEST_MODE"])

			self.b_do_csv_chunk = bool(d_settings["DO_CSV_CHUNK"])
			self.chunk_size = int(d_settings["CHUNK_SIZE"])
			
			self.first_jsunnoon = int(d_settings["FIRST_JSUNNOON"])
		
			self.encr_key = int(d_settings["ENCR_KEY"], base=2)
		
			self.b_modify_ohlcv_flag = bool(d_settings["MODIFY_OHLCV_FLAG"])
			
			self.fifo_count = int(d_settings["FIFO_COUNT"])
			
			self.b_process_L31 = bool(d_settings["PROCESS_L31"])
			self.b_process_L32 = bool(d_settings["PROCESS_L32"])
			self.b_process_L33 = bool(d_settings["PROCESS_L33"])
			self.b_process_L34 = bool(d_settings["PROCESS_L34"])
			self.b_process_L35 = bool(d_settings["PROCESS_L35"])
			self.b_process_L36 = bool(d_settings["PROCESS_L36"])
			self.b_process_L37 = bool(d_settings["PROCESS_L37"])
			self.b_process_L38 = bool(d_settings["PROCESS_L38"])
			self.b_process_L48 = bool(d_settings["PROCESS_L48"])
			self.b_process_L58 = bool(d_settings["PROCESS_L58"])
			self.b_process_L59 = bool(d_settings["PROCESS_L59"])
			self.b_process_L68 = bool(d_settings["PROCESS_L68"])
			self.b_process_L69 = bool(d_settings["PROCESS_L69"])
			self.b_process_L79 = bool(d_settings["PROCESS_L79"])
			self.b_process_L88 = bool(d_settings["PROCESS_L88"])
			self.b_process_L98 = bool(d_settings["PROCESS_L98"])
			
			self.gmt_offset_storloc = int(d_settings["GMT_OFFSET_STORLOC"])
			self.trading_start_recno_storloc = int(d_settings["TRADING_START_RECNO_STORLOC"])
			self.trading_recs_perday_storloc = int(d_settings["TRADING_RECS_PERDAY_STORLOC"])
			self.idf_currency_storloc = int(d_settings["IDF_CURRENCY_STORLOC"])
			self.tick_storloc = int(d_settings["TICK_STORLOC"])
			self.ohlc_divider_storloc = int(d_settings["OHLC_DIVIDER_STORLOC"])
			self.last_fced_recno_storloc = int(d_settings["LAST_FCED_RECNO_STORLOC"])
			self.split_factor_storloc = int(d_settings["SPLIT_FACTOR_STORLOC"])
			self.currency_value_of_point_storloc = int(d_settings["CURRENCY_VALUE_OF_POINT_STORLOC"])
			self.highest_recno = dc.Decimal(d_settings["HIGHEST_RECNO"])
			self.highest_recno_close_storloc = int(d_settings["HIGHEST_RECNO_CLOSE_STORLOC"])
			self.prev_highest_recno_close_storloc = int(d_settings["PREV_HIGHEST_RECNO_CLOSE_STORLOC"])
		except KeyError as keyerr:
			log.error("Could not find required key %s in settings file." % str(keyerr))
			raise
		except:
			raise

	def  get_dd_data_root(self):
		if self.b_test_mode:
			return self.s_local_dd_data_root
		return self.s_dd_data_root

	def get_s3_data_root(self):
		if self.b_test_mode:
			return self.s_local_s3_data_root
		return self.s_s3_data_root

	def get_trading_start_recno(self):
		return self.trading_start_recno

	def get_ddstore(self):
		if self.ddstore:
			return self.ddstore

		log.debug("Creating DDStore")
		from dd import DDStore
		ddstore = DDStore(self.s_dd_access_key,
							self.s_dd_secret_access_key,
							self.s_dd_region,
							self.dd_read_units,
							self.dd_write_units)

		self.ddstore = ddstore
		return ddstore