import datetime as dt
import os.path
import re
import decimal as dc
import argparse
import sys
import glob

from odfexcept import *
import fifo
import odf
from fifo import FIFO
from odf import ODF

import logging
import logging.handlers
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

def get_working_dir():
	return os.path.dirname(__file__)

def joinpaths(ls_components):

	s_path = os.path.join(ls_components[0], ls_components[1])
	for i in range(2, len(ls_components)):
		s_path = os.path.join(s_path, ls_components[i])

	return s_path


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

	def get_symbol(self, s_odf_basename):
		m = re.match(r'^(.*)\-.*$', s_odf_basename)

		if not m:
			raise ODFException("Invalid ODF Name %s" % s_odf_basename)

		return m.group(1)

	def get_working_dir(self):
		s_working_dir = os.path.dirname(__file__)
		return s_working_dir

	def refresh_fifo_local(self, odf_obj, s_fifo_dd):
		config = self.config
		fifo_obj = None
		if os.path.exists(s_fifo_dd):
			dt_fifo_mtime = dt.datetime.fromtimestamp(os.path.getmtime(s_fifo_dd))
			dt_now = dt.datetime.now()
			dt_delta = dt_now - dt_fifo_mtime

			if dt_delta > dt.timedelta(25):
				log.debug("Updating FIFO (older than 25 days)...")

				l_fifo_arr = odf_obj.get_fifo_arr(config.fifo_count,
													config.trading_start_recno)
				
				fifo_obj = fifo.open_fifo_bin(s_fifo_dd)
				fifo_obj.update_from_list(l_fifo_arr)
				fifo_obj.store_header()

				fifo_obj.to_bin_file(s_fifo_dd)
			else:
				log.debug("FIFO is too recent (<25 days old). Skipping...")
		else:

			l_fifo_arr = odf_obj.get_fifo_arr(config.fifo_count,
												config.trading_start_recno)

			fifo_obj = FIFO()
			fifo_obj.read_list(l_fifo_arr)
			fifo_obj.to_bin_file(s_fifo_dd)	

		return fifo_obj

	def refresh_fifo_dd(self, odf_obj, s_fifo_dd):
		return None

	def refresh_fifo(self, odf_obj, s_fifo_dd):
		if self.config.b_test_mode:
			return self.refresh_fifo_local(odf_obj, s_fifo_dd)
		else:
			return self.refresh_fifo_dd(odf_obj, s_fifo_dd)

	def print_fifo_local(self, odf_obj, s_fifo_dd):

		fifo_obj = FIFO()
		fp_fifo_dd = open(s_fifo_dd, "rb")
		fifo_obj.read_bin_stream(fp_fifo_dd)
		fp_fifo_dd.close()

		buf = str(fifo_obj)

		log.debug("FIFO %s TEXT:\n%s\n" % (s_fifo_dd, buf))

	def fill_missing_odf_header_records(self, odf_obj, s_odf_dd, fifo_obj, fce_obj, prev_fce_obj):

		config = self.config

		odf_tick = odf_obj.get_header_value(config.tick_storloc)
		odf_ohlc_divider = odf_obj.get_header_value(config.ohlc_divider)
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
		odf_tick = fifo_obj.get_tick()
		odf_ohlc_divider = fifo_obj.get_ohlc_divider()
		odf_last_fced_recno = 0
		odf_highest_recno = odf_obj.find_highest_recno(config.trading_start_recno, 
														config.trading_recs_perday)
		odf_highest_recno_close = odf_obj.get_field(odf_highest_recno, 'ODF_CLOSE')


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

		# To do: save the ODF
		if self.b_test_mode:
			odf_obj.to_bin_file(s_odf_dd)

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

	def get_current_jsunnoon(self):
		return None

	def process_fce(fce_spec, fce_obj, odf_obj, s_odf_s3, l_array_fce_intervals):
		odf_jsunnoon = fce_spec.odf_jsunnoon
		config = self.config
		if fce_obj == None:
			fce_obj = fce.FCE(config)
			fce_obj.save_to_s3(config, fce_spec.s_fce_header_filename)
		current_jsunnoon = self.get_current_jsunnoon()

		if config.last_fced_recno == config.highest_recno and not odf_jsunnoon == current_jsunnoon:
			odf_obj.save_to_s3()
			return
		
		if config.last_fced_recno == self.highest_recno:
			return
		
		odf_recno = config.last_fced_recno
		l_chunk_arr_list = None

		while True:
			odf_recno += 1
			if odf_obj.is_recno_out_of_limits(odf_recno):
				continue

			l_chunk_arr_list = self.write_chunk_arr(odf_recno, odf_obj, l_chunk_arr_list, l_array_fce_intevals)

			if odf_recno >= config.highest_recno:
				break

		l_chunk_arr_short_list - self.get_chunk_arr_short_list(l_chunk_arr_list)

		self.write_chunk_files(l_chunk_arr_short_list)

		odf_obj.set_header_value('LAST_FCED_RECNO', config.highest_recno)




	def odf2fce(self):
		''' Processes ODFs on local storage or S3 and updates FCEs on local storage or DD
		'''
		log.info(dt.datetime.now())
		log.info("-----CYCLE START-----")
		
		''' Get a handle to the configuration
		'''
		config = self.config

		''' Get path to the S3 storage
		'''
		s_s3_path = config.get_s3_data_root()

		''' Get current working directory
		'''		
		s_app_dir = self.get_working_dir()

		''' Get the number of exchanges we have to process
		'''
		ls_exchanges = self.list_exchanges()


		for s_exchange in ls_exchanges:
			
			s_exchange_basename = self.get_exchange_basename(s_exchange)
			''' If we cannot process data, skip
			'''
			if not config.b_process_data:
				continue

			''' Get the number of odf's in i-th exchange
			'''
			ls_odf_names = self.list_odfs(s_exchange)

			for s_odf_dd in ls_odf_names:

				s_odf_basename = self.get_odf_basename(s_odf_dd)

				''' Extract symbol name from ODF basename (string before first '_')
				'''
				s_symbol = self.get_symbol(s_odf_basename)

				''' Make FIFO name for this ODF from the ODF's basename
				'''
				s_fifo_dd = self.get_fifo_dd(s_exchange, s_odf_basename)
				s_fifo_basename = self.get_fifo_basename(s_odf_basename)

				''' Get path to the FCE for this ODF on S3
				''' 
				#s_fce_s3 = fce.get_fce_s3(s_s3_path=s_s3_path, s_odf_exchange=s_odf_exchange, s_symbol=s_symbol)
				fce_spec = fce.FCESpec(s_odf_basename)
				prev_fce_obj = fce.open_fce(s_exchange_basename, s_symbol, s_fce_spec.s_prev_fce_header_filename, config)
				fce_obj = fce.open_fce(s_exchange_basename, s_symbol, fce_spec.s_fce_header_filename, config)

				''' We first need to update FCE on local storage before uploading,
				so get path to where it will be stored locally.
				'''
				s_fce_tmp = joinpaths([s_app_dir, 'tmp', s_exchange, 'fce', s_symbol])
				s_fce_dir = os.path.dirname(s_fce_tmp)
				if not os.path.exists(s_fce_dir):
					os.makedirs(s_fce_dir)

				''' Print the ODF path on DD if required
				'''
				if self.b_show:
					if config.b_test_mode:
						log.info(s_odf_dd)
					else:
						log.info(':'.join([s_exchange, s_odf_dd]))

				if config.b_test_mode:
					odf_obj = odf.open_odf_bin(s_odf_dd)
				else:
					odf_obj = odf.open_odf_dd(s_exchange, s_odf_dd)

				if config.b_test_mode and self.b_print_mode:
					self.print_fifo_local(s_exchange_basename, s_odf_dd, s_fifo_dd)
					continue

				''' Refresh the FIFO from ODF data
				'''
				fifo_obj = self.refresh_fifo(odf_obj, s_fifo_dd)

				self.fill_missing_odf_header_records(odf_obj, s_odf_dd, fifo_obj, fce_obj, prev_fce_obj)

				self.fill_missing_odf_records(odf_obj)

				l_array_fce_intervals = self.fill_fce_intervals_array()

				#self.process_fce(s_odf_dd, s_dd_path, s_odf_s3, s_fce_s3, l_array_fce_intervals)


		log.info(dt.datetime.now())
		log.info("-----CYCLE DONE-----")

	
	def list_exchanges(self):
		''' Get a handle to the configuration
		'''
		config = self.config
		ls_exchanges = []
		''' Get path to the DynamoDB storage
		'''

		if config.b_test_mode:
			s_root_dir = config.get_dd_data_root()
			s_root_glob = os.sep.join([s_root_dir, '*'])
			ls_exchanges = glob.glob(s_root_glob)
		else:
			ddstore = config.get_ddstore()
			ls_exchanges = ddstore.list_exchanges()

		return ls_exchanges

	def get_exchange_basename(self, s_exchange):
		if self.config.b_test_mode:
			return os.path.basename(s_exchange)
		return s_exchange

	def get_odf_basename(self, s_odf_name):
		if self.config.b_test_mode:
			return re.sub(r'\.rs3', '', os.path.basename(s_odf_name))
		return s_odf_name

	def get_fifo_dd(self, s_exchange, s_odf_basename):
		if self.config.b_test_mode:
			s_fifo_dd = '.'.join([s_odf_basename, 'fif'])
			s_root_dir = os.path.dirname(s_exchange)
			s_root_dir = os.path.dirname(s_root_dir)
			s_root_dir = os.sep.join([s_root_dir, 'fifo'])
			s_root_dir = os.sep.join([s_root_dir, os.path.basename(s_exchange)])
			s_fifo_dd = os.sep.join([s_root_dir, s_fifo_dd])
			s_fifo_dir = os.path.dirname(s_fifo_dd)
			if not os.path.exists(s_fifo_dir):
				os.makedirs(s_fifo_dir)
			return s_fifo_dd
		return s_odf_basename

	def get_fifo_basename(self, odf_basename):
		return odf_basename

	def list_odfs(self, s_exchange):
		''' Get a handle to the configuration
		'''
		config = self.config

		''' Get path to the DynamoDB storage
		'''
		ls_odf_names = []
		if config.b_test_mode:
			s_exchange_glob = os.sep.join([s_exchange, '*.rs3'])
			ls_odf_names = glob.glob(s_exchange_glob)
		else:
			ddstore = config.get_ddstore()
			ls_odf_names = ddstore.list_odfs(s_exchange)		

		return ls_odf_names

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
			self.config = Config()

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

