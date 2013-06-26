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

# Required by argument processor
import sys
import os.path
import re
import glob
from odf import ODF

# Create logger
import logging
log = logging.getLogger(__name__)


class ODFProcessor:

	def __init__(self, ddstore=None):
		self.ddstore = ddstore

	def txt2bin(self, s_txt_src, s_bin_dst):

		odf = ODF()

		fp_txt_odf = open(s_txt_src, "r")

		odf.read_text_stream(fp_txt_odf)

		fp_txt_odf.close()

		buf = odf.to_bin()

		fp_bin_odf = open(s_bin_dst, "wb")

		fp_bin_odf.write(buf)

		fp_bin_odf.close()

	def print_bin(self, s_bin_src):
		odf = ODF()

		fp_bin_odf = open(s_bin_src, "rb")

		odf.read_bin_stream(fp_bin_odf)

		fp_bin_odf.close()

		print(odf)

	def convert_txt2bin(self, s_exchange, s_txt_src, b_show=False):
		s_out_dir = os.path.dirname(s_txt_src)
		s_bin_dst = os.path.basename(s_txt_src)
		s_bin_dst = re.sub(r'rs4', r'rs3', s_bin_dst)
		s_bin_dst = os.path.join(s_out_dir, s_bin_dst)

		if b_show:
			log.info("%s" % s_txt_src)
			#log.info("Destin : %s" % s_bin_dst)
		
		self.txt2bin(s_txt_src, s_bin_dst)

	def load_odf_txt(self, s_txt_src):
		odf = ODF()

		fp_txt_odf = open(s_txt_src, "r")

		odf.read_text_stream(fp_txt_odf)

		fp_txt_odf.close()

		return odf

	def load_odf_bin(self, s_bin_src):
		odf = ODF()

		fp_bin_odf = open(s_bin_src, "rb")

		odf.read_bin_stream(fp_bin_odf)

		fp_bin_odf.close()

		return odf

	def convert_txt2dd(self, s_exchange, odf_table, s_txt_src, b_show=False):

		odf = self.load_odf_txt(s_txt_src)

		s_odf_basename = os.path.basename(s_txt_src)
		s_odf_basename = re.sub(r'\.rs4', '', s_odf_basename)
		
		log.debug("Converting ODF to table records")
		ld_odf_recs = odf.to_dict(s_odf_basename)

		#log.debug(str(ld_odf_recs) + "\n\n")
		
		if b_show:
			log.info("%s" % s_txt_src)
			

		log.debug("Loading table from file: %s" % s_txt_src)
		self.ddstore.put_records_multi(odf_table, ld_odf_recs)
		log.debug("Moved file: %s" % s_txt_src)

	def convert_bin2dd(self, s_exchange, odf_table, s_bin_src, b_show=False):

		odf = self.load_odf_bin(s_bin_src)

		s_odf_basename = os.path.basename(s_bin_src)
		s_odf_basename = re.sub(r'\.rs3', '', s_odf_basename)
		
		log.debug("Converting ODF to table records")
		ld_odf_recs = odf.to_dict(s_odf_basename)

		#log.debug(ld_odf_recs)

		log.debug("Loading table from file: %s" % s_bin_src)
		self.ddstore.put_records_multi(odf_table, ld_odf_recs)
		log.debug("Moved file: %s" % s_bin_src)
		

	def for_all_odfs_bin(self, s_root_dir, fn_do, b_show=False):
		s_root_glob = os.sep.join([s_root_dir, '*'])
		ls_exchanges = glob.glob(s_root_glob)

		#log.debug(ls_exchanges)
		for s_exchange in ls_exchanges:
			s_exchange_glob = os.sep.join([s_exchange, '*.rs3'])
			ls_rs3s = glob.glob(s_exchange_glob)

			for s_rs3 in ls_rs3s:
				fn_do(s_exchange, s_rs3, b_show)

	def for_all_odfs_bin_dd(self, config, fn_do, b_show=False):

		ls_exchanges = self.ddstore.list_exchanges()

		for s_exchange in ls_exchanges:
			ls_odf_names = self.ddstore.list_odfs(s_exchange)

			for s_odf_name in ls_odf_names:
				fn_do(config, s_exchange, s_odf_name, b_show)

	def for_all_odfs_txt(self, s_root_dir, fn_do, b_show=False, b_dd_out=False):
		s_root_glob = os.sep.join([s_root_dir, '*'])
		ls_exchanges = glob.glob(s_root_glob)

		#log.debug(ls_exchanges)
		for s_exchange in ls_exchanges:
			s_exchange_glob = os.sep.join([s_exchange, '*.rs4'])
			ls_rs4s = glob.glob(s_exchange_glob)
			s_exchange_basename = os.path.basename(s_exchange)
			odf_table =None
			try:

				if b_dd_out:
					log.debug("Creating/getting table : %s" % s_exchange_basename)
					odf_table = self.ddstore.get_table(s_table_name=s_exchange_basename,
														**ODF.d_odf_dd_schema)

				for s_rs4 in ls_rs4s:
					if b_dd_out:
						fn_do(s_exchange_basename, odf_table, s_rs4, b_show)
					else:	
						fn_do(s_exchange_basename, s_rs4, b_show)

			finally:
				log.debug("Resetting table throughput.")
				if b_dd_out:
					self.ddstore.toggle_write_throughput(odf_table)

