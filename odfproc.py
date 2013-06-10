
# Required by argument processor
import sys
import os.path
import re
import glob
from odf import ODF


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

		print odf

	def convert_txt2bin(self, s_exchange, s_txt_src):
		s_out_dir = os.path.dirname(s_txt_src)
		s_bin_dst = os.path.basename(s_txt_src)
		s_bin_dst = re.sub(r'rs4', r'rs3', s_bin_dst)
		s_bin_dst = os.path.join(s_out_dir, s_bin_dst)

		print "Source : %s" % s_txt_src
		print "Destin : %s" % s_bin_dst
		
		self.txt2bin(s_txt_src, s_bin_dst)
		#print_bin(s_bin_dst)

	def load_odf_txt(self, s_txt_src):
		odf = ODF()

		fp_txt_odf = open(s_txt_src, "r")

		odf.read_text_stream(fp_txt_odf)

		fp_txt_odf.close()

		return odf

	def load_odf_bin(self, s_bin_src):
		odf = ODF()

		fp_txt_odf = open(s_bin_src, "rb")

		odf.read_bin_stream(fp_bin_odf)

		fp_bin_odf.close()

		return odf

	def convert_txt2dd(self, s_exchange, s_txt_src):

		odf = self.load_odf_txt(s_txt_src)

		s_odf_basename = os.path.basename(s_txt_src)
		s_odf_basename = re.sub(r'\.rs4', '', s_odf_basename)
		
		print "Converting ODF to table records"
		ld_odf_recs = odf.to_dict(s_odf_basename)

		#print str(ld_odf_recs) + "\n\n"
		
		print "Creating/getting table : %s" % s_exchange
		odf_table = self.ddstore.create_odf_table(s_exchange)

		print "Loading table from file: %s" % s_txt_src
		self.ddstore.put_odf_records(odf_table, ld_odf_recs)
		

		print "Moved file: %s" % s_txt_src

	def convert_bin2dd(self, s_exchange, s_bin_src):

		odf = self.load_odf_bin(s_bin_src)

		s_odf_basename = os.path.basename(s_bin_src)
		s_odf_basename = re.sub(r'\.rs3', '', s_odf_basename)
		
		print "Converting ODF to table records"
		ld_odf_recs = odf.to_dict(s_odf_basename)

		#print ld_odf_recs
		print "Creating/getting table : %s" % s_exchange
		odf_table = self.ddstore.create_odf_table(s_exchange)

		print "Loading table from file: %s" % s_bin_src
		self.ddstore.put_odf_records(odf_table, ld_odf_recs)

		print "Moved file: %s" % s_bin_src
		

	def for_all_odfs_bin(self, s_root_dir, fn_do):
		s_root_glob = os.sep.join([s_root_dir, '*'])
		ls_exchanges = glob.glob(s_root_glob)

		print ls_exchanges
		for s_exchange in ls_exchanges:
			s_exchange_glob = os.sep.join([s_exchange, '*.rs3'])
			ls_rs3s = glob.glob(s_exchange_glob)

			for s_rs3 in ls_rs4s:
				fn_do(s_exchange, s_rs3)

	def for_all_odfs_txt(self, s_root_dir, fn_do):
		s_root_glob = os.sep.join([s_root_dir, '*'])
		ls_exchanges = glob.glob(s_root_glob)

		print ls_exchanges
		for s_exchange in ls_exchanges:
			s_exchange_glob = os.sep.join([s_exchange, '*.rs4'])
			ls_rs4s = glob.glob(s_exchange_glob)
			s_exchange_basename = os.path.basename(s_exchange)
			for s_rs4 in ls_rs4s:
				fn_do(s_exchange_basename, s_rs4)

