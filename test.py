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
		self.assertEqual(jsn, 56460)

class ODFTests(unittest.TestCase):

	def setUp(self):
		import odf2fce
		import config
		import dd
		import s3
		import os.path
		import re
		print(__file__)

		s_name = os.path.basename(re.sub(r'\.py', '', __file__))
		
		self.o2f = odf2fce.Odf2Fce()
		self.o2f.initialize_logging(s_name)

		log.debug("Setting up test rig")

		self.config = config.Config()
		self.config.read_settings("settings.txt") 
		self.dd_store = dd.get_dd_store(self.config)
		self.s3_store = s3.get_s3_store(self.config)
		self.o2f.s3_store = self.s3_store
		self.o2f.dd_store = self.dd_store

		log.debug("test setup complete")

	def test_odf_read(self):
		""" Validates Generated binary files against text odfs
		"""
		log.debug("test_odf_read")

		# Load the test odf
		ls_exchanges = self.dd_store.list_exchanges()

		s_test_exchange = ls_exchanges[0]

		ls_odf_names = self.dd_store.list_odfs(s_test_exchange)

		s_test_odf = ls_odf_names[0]

		s_test_txt_odf = re.sub(r'\.rs3', '.rs4', s_test_odf)
		
		odf_obj = self.dd_store.open_odf(s_test_exchange, s_test_odf)

		# Load the text odf against which to compare
		fp_odf_text = open(s_test_txt_odf, "r")

		# First check the headers
		for i in range(7):
			line = fp_odf_text.readline().strip()
			text_value = int(line)
			bin_value = odf_obj.get_header_value(i+1)
			self.assertEqual(bin_value, text_value)

		# Now check the body
		while True:
			line = fp_odf_text.readline().strip()

			if line is None or line == "":
				break

			ls_values = line.split(',')

			rn = int(ls_values[0].strip())
			op = dc.Decimal(ls_values[1].strip())
			hi = dc.Decimal(ls_values[2].strip())
			lo = dc.Decimal(ls_values[3].strip())
			cl = dc.Decimal(ls_values[4].strip())
			vo = dc.Decimal(ls_values[5].strip())

			self.assertEqual(odf_obj.get_field(rn, 'ODF_OPEN'), op)
			self.assertEqual(odf_obj.get_field(rn, 'ODF_HIGH'), hi)
			self.assertEqual(odf_obj.get_field(rn, 'ODF_LOW'), lo)
			self.assertEqual(odf_obj.get_field(rn, 'ODF_CLOSE'), cl)
			self.assertEqual(odf_obj.get_field(rn, 'ODF_VOLUME'), vo)

		fp_odf_text.close()


if __name__ == '__main__':
	unittest.main()
