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
import re
import os.path
import shutil
import decimal as dc

import unittest

import logging
log = logging.getLogger(__name__)


class BasicTests(unittest.TestCase):
	
	def setUp(self):
		pass

	def test_jsunnoon(self):

		import odf2fce
		o2f = odf2fce.Odf2Fce()
		jsn = o2f.get_current_jsunnoon()
		# Put value here for most recent sunday using http://www.nr.com/julian.html
		self.assertEqual(jsn, 56467)

class ODFFIFOTests(unittest.TestCase):
	""" ODF and FIFO unit tests.
	"""
	def __init__(self, *kargs, **kwargs):
		super(ODFFIFOTests, self).__init__(*kargs, **kwargs)

	def skipOrRunTest(self, testType):
		#testsToRun = 'test_fill_missing_odf_records'
		testsToRun = 'test_process_fce'
		#testsToRun = 'test_odf_read'
		#testsToRun = 'test_encryption'
		if ((testsToRun == 'ALL') or (testType in testsToRun)):
			return True
		else:
			log.debug("SKIPPED TEST %s" % testType)
			self.skipTest("Skipped!")

	def setup_odf(self):
		# Load the test exchange names
		self.ls_exchanges = self.dd_store.list_exchanges()
	
	def setup_environment(self):
		# Load the base test environment
		import odf2fce
		import config
		import dd
		import s3
		import os.path
		import re

		s_name = os.path.basename(re.sub(r'\.py', '', __file__))
		
		self.o2f = odf2fce.Odf2Fce()

		self.o2f.initialize_logging(s_name)

		log.debug("Setting up test rig")

		self.config = config.Config()
		self.config.read_settings("settings.txt") 
		self.dd_store = dd.get_dd_store(self.config)
		self.s3_store = s3.get_s3_store(self.config)
		self.o2f.config = self.config
		self.o2f.s3_store = self.s3_store
		self.o2f.dd_store = self.dd_store

		log.debug("Set up basic test rig")
		

	def setUp(self):
		
		self.setup_environment()
		self.setup_odf()
		log.debug("test setup complete")

	def test_encryption(self):
		""" Tests encryption
		"""
		self.skipOrRunTest('test_encryption')
		log.info("test_encryption: begin")
		encr_key  = self.config.encr_key

		s_test_string = "Hello, world! Mr. Snowden, I presume."
		import binary

		s_test_buf = bytes(s_test_string, 'utf-8')

		log.info("Plaintext string: %s" % s_test_string)
		log.info("Plaintext bytes: %d" % len(s_test_buf))

		s_encrypted = binary.xor_crypt(s_test_buf, encr_key)
		
		log.info("Encrypted bytes: %d" % len(s_encrypted))
		
		s_decrypted = binary.xor_crypt(s_encrypted, encr_key).decode('utf-8')

		log.info("Decrypted bytes: %d" % len(s_decrypted))

		log.info("Decrypted string: %s" % s_decrypted)

		self.assertTrue(s_test_string, s_decrypted)


		log.info("test_encryption: complete")

	def test_odf_read(self):
		""" Validates Generated binary files against text odfs
		"""
		self.skipOrRunTest('test_odf_read')
		log.info("test_odf_read: begin")

		for s_test_exchange in self.ls_exchanges:
			ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
			for s_test_odf in ls_odf_names:		
				odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)

				s_test_txt_odf = re.sub(r'\.rs3', '.rs4', s_test_odf)

				log.info("Binary ODF: %s" % s_test_odf)
				log.info("Text   ODF: %s" % s_test_txt_odf)

				# Load the text odf against which to compare
				fp_odf_text = open(s_test_txt_odf, "r")

				# First check the headers
				for i in range(7):
					line = fp_odf_text.readline().strip()
					text_value = int(line)
					bin_value = odf_obj.get_header_value(i+1)
					self.assertEqual(bin_value, text_value)

				# Now check the body
				d_dup_dict = {}

				while True:
					line = fp_odf_text.readline().strip()

					if line is None or line == "":
						break

					ls_values = line.split(',')
					d_rec = {}
					rn = int(ls_values[0].strip())
					d_rec['rn'] = rn
					d_rec['op'] = dc.Decimal(ls_values[1].strip())
					d_rec['hi'] = dc.Decimal(ls_values[2].strip())
					d_rec['lo'] = dc.Decimal(ls_values[3].strip())
					d_rec['cl'] = dc.Decimal(ls_values[4].strip())
					d_rec['vo'] = dc.Decimal(ls_values[5].strip())

					d_dup_dict[rn] = d_rec

				fp_odf_text.close()

				for rn, d_rec in d_dup_dict.items():
					try:
						self.assertEqual(odf_obj.get_field(rn, 'ODF_OPEN'), d_rec['op'])
						self.assertEqual(odf_obj.get_field(rn, 'ODF_HIGH'), d_rec['hi'])
						self.assertEqual(odf_obj.get_field(rn, 'ODF_LOW'), d_rec['lo'])
						self.assertEqual(odf_obj.get_field(rn, 'ODF_CLOSE'), d_rec['cl'])
						self.assertEqual(odf_obj.get_field(rn, 'ODF_VOLUME'), d_rec['vo'])
					except:
						log.exception("Error for recno: %d" % rn)
						raise
				
		log.info("test_odf_read: complete.")

	def test_refresh_fifo(self):
		""" Test if a FIFO is generated correctly from ODF
		"""
		self.skipOrRunTest('test_refresh_fifo')
		log.info("test_refresh_fifo: begin")

		for s_test_exchange in self.ls_exchanges:
			ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
			for s_test_odf in ls_odf_names:
				s_test_odf_basename = self.dd_store.get_odf_basename(s_test_odf)		
				odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)

				s_fifo_dir = self.dd_store.get_fifo_dir(s_test_exchange, 
														s_test_odf_basename)

				if os.path.exists(s_fifo_dir):
					shutil.rmtree(s_fifo_dir)
				
				s_fifo_dd = self.dd_store.get_fifo_path(s_test_exchange, 
														s_test_odf_basename)

				s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

				fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

				self.assertNotEqual(fifo_obj, None)

		log.info("test_refresh_fifo: complete")

	def test_re_refresh_fifo(self):
		""" Check if an existing fifo is handled correctly and reloaded
		"""
		self.skipOrRunTest('test_re_refresh_fifo')
		log.info("test_re_refresh_fifo: begin")
		for s_test_exchange in self.ls_exchanges:
			ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
			for s_test_odf in ls_odf_names:
				s_test_odf_basename = self.dd_store.get_odf_basename(s_test_odf)		
				odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)
				s_fifo_dir = self.dd_store.get_fifo_dir(s_test_exchange, 
														s_test_odf_basename)

				if os.path.exists(s_fifo_dir):
					shutil.rmtree(s_fifo_dir)
				
				s_fifo_dd = self.dd_store.get_fifo_path(s_test_exchange, 
														s_test_odf_basename)

				s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

				fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

				self.assertNotEqual(fifo_obj, None)	
				
				fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)		

				self.assertNotEqual(fifo_obj, None)

		log.info("test_re_refresh_fifo: complete")

	def test_re_refresh_old_fifo(self):
		""" Check if a > 25 day old fifo is handled correctly.
		"""
		self.skipOrRunTest('test_re_refresh_old_fifo')
		log.info("test_re_refresh_old_fifo: begin")
		for s_test_exchange in self.ls_exchanges:
			ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
			for s_test_odf in ls_odf_names:
				s_test_odf_basename = self.dd_store.get_odf_basename(s_test_odf)		
				odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)
				s_fifo_dir = self.dd_store.get_fifo_dir(s_test_exchange, 
														s_test_odf_basename)

				if os.path.exists(s_fifo_dir):
					shutil.rmtree(s_fifo_dir)
				
				s_fifo_dd = self.dd_store.get_fifo_path(s_test_exchange, 
														s_test_odf_basename)

				s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

				fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

				self.assertNotEqual(fifo_obj, None)	
				
				# Now set the mtime of the fifo to a date 25 days before
				self.dd_store.b_force_fifo_old = True

				fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)		

				self.assertNotEqual(fifo_obj, None)

		log.info("test_re_refresh_old_fifo: complete")

	def test_fill_missing_odf_records(self):
		self.skipOrRunTest('test_fill_missing_odf_records')
		log.info("test_fill_missing_odf_records: begin")
		#for s_test_exchange in self.ls_exchanges:
		s_test_exchange = self.ls_exchanges[1]
		s_test_exchange_basename = self.dd_store.get_exchange_basename(s_test_exchange)
		ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
		#for s_test_odf in ls_odf_names:
		s_test_odf = ls_odf_names[0]
		log.info(s_test_odf)

		s_app_dir = self.o2f.get_working_dir()
		s_test_odf_basename = self.dd_store.get_odf_basename(s_test_odf)
		s_symbol = self.dd_store.get_odf_symbol(s_test_odf_basename)

		self.dd_store.clear_state(s_test_exchange, s_test_odf_basename)
		self.s3_store.clear_state(s_app_dir, s_test_exchange_basename, s_symbol, s_test_odf_basename)


		odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)
		
		s_fifo_dd = self.dd_store.get_fifo_path(s_test_exchange, 
												s_test_odf_basename)

		s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

		fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

		self.assertNotEqual(fifo_obj, None)

		
		fce_pathspec = self.s3_store.get_fce_pathspec(s_app_dir,
														s_test_exchange_basename,
														s_symbol,
														s_test_odf_basename)

		self.o2f.fill_missing_odf_header_records(odf_obj, s_test_odf, s_test_odf_basename, fifo_obj, fce_pathspec)

		self.o2f.fill_missing_odf_records(odf_obj)

		log.info("test_fill_missing_odf_records: complete")

	def test_process_fce(self):
		self.skipOrRunTest('test_process_fce')
		log.info("test_process_fce: begin")
		#for s_test_exchange in self.ls_exchanges:
		s_test_exchange = self.ls_exchanges[0]
		s_test_exchange_basename = self.dd_store.get_exchange_basename(s_test_exchange)
		ls_odf_names = self.dd_store.list_odfs(s_test_exchange)
		#for s_test_odf in ls_odf_names:
		s_test_odf = ls_odf_names[1]
		log.info(s_test_odf)
		s_test_odf_basename = self.dd_store.get_odf_basename(s_test_odf)

		s_app_dir = self.o2f.get_working_dir()
		s_symbol = self.dd_store.get_odf_symbol(s_test_odf_basename)
		
		self.dd_store.clear_state(s_test_exchange_basename, s_test_odf_basename)
		self.s3_store.clear_state(s_app_dir, s_test_exchange_basename, s_symbol, s_test_odf_basename)

		odf_obj = self.dd_store.open_odf(s_test_exchange_basename, s_test_odf)



		s_fifo_dir = self.dd_store.get_fifo_dir(s_test_exchange, 
												s_test_odf_basename)

		if os.path.exists(s_fifo_dir):
			shutil.rmtree(s_fifo_dir)
		
		
		s_fifo_dd = self.dd_store.get_fifo_path(s_test_exchange_basename, 
												s_test_odf_basename)

		s_fifo_basename = self.dd_store.get_fifo_basename(s_fifo_dd)

		fifo_obj = self.o2f.refresh_fifo(odf_obj, s_fifo_dd, s_fifo_basename)

		self.assertNotEqual(fifo_obj, None)

		fce_pathspec = self.s3_store.get_fce_pathspec(s_app_dir,
														s_test_exchange_basename,
														s_symbol,
														s_test_odf_basename)

		self.o2f.fill_missing_odf_header_records(odf_obj, s_test_odf, s_test_odf_basename, fifo_obj, fce_pathspec)

		self.o2f.fill_missing_odf_records(odf_obj)

		l_array_fce_intervals = self.o2f.fill_fce_intervals_array()

		s_test_odf_s3 = self.s3_store.get_odf_path(s_test_exchange_basename, s_test_odf_basename)
		
		self.o2f.process_fce(fce_pathspec, s_test_odf, s_test_odf_basename, odf_obj, s_test_odf_s3, l_array_fce_intervals)

		log.info("test_process_fce: complete")


if __name__ == '__main__':
	unittest.main()
