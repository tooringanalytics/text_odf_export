
# Library imports
import sys
import os.path
import re
import glob
import argparse

# Local imports
from dd import DDStore
from odf import ODF
from odfproc import ODFProcessor


def test_conversion():
	odf = ODF()

	fp_txt_odf = open("EURUSD-1855053.rs4", "r")

	odf.read_text_stream(fp_txt_odf)

	fp_txt_odf.close()

	buf = odf.to_bin()

	fp_bin_odf = open("odf_bin.bin", "wb")

	fp_bin_odf.write(buf)

	fp_bin_odf.close()

	odf2 = ODF()

	fp_bin_odf = open("odf_bin.bin", "rb")

	odf.read_bin_stream(fp_bin_odf)

	fp_bin_odf.close()

	print odf

class TextODFExporter(object):

	re_reply_yes = re.compile(r'^(y|(yes))$', re.I)

	def __init__(self):
		self.parser = argparse.ArgumentParser(description='Text File to ODF Exporter.')

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
		
		parser.add_argument('-b', '--binary', 
							action='store_true',
	                   		help='Convert text to packed binary, and store locally.')
		
		parser.add_argument('-d', '--dynamodb',
							action='store_true',
							help='Upload ODF data from text file to AWS DynamoDB.')
		
		return parser.parse_args()

	def set_args(self, args):
		""" Set internal variables according to results parsed from command line
		@param args: argparse object containing parse results. 
		"""
		
		self.b_ask = True
		self.b_show = False
		self.b_binary = True
		self.b_dynamodb = False

		if hasattr(args, "ask") and args.ask is not None:
			self.b_ask = args.ask
		
		if hasattr(args, "show") and args.show is not None:
			self.b_show = args.show
		
		if hasattr(args, "binary") and args.binary is not None:
				self.b_binary = args.binary

		if hasattr(args, "dynamodb") and args.dynamodb is not None:
			self.b_dynamodb = args.dynamodb
		
		'''Do some sanity checks on the arguments.
		'''
		if self.b_binary and self.b_dynamodb:
			self.parser.error('Cannot combine -b with -d')

		# Should not happen, but handle this anyway.
		if not self.b_binary and not self.b_dynamodb:
			self.parser.error("Must use one of -b or -d")

	
	def prompt_interactive(self):
		s_reply = ""

		if self.b_binary:
			s_reply = raw_input("This program will generate binary ODFs. Proceed? (y/N): ").strip()
		elif self.b_dynamodb:
			s_reply = raw_input("This program will upload ODF data to DynamoDB. Proceed? (y/N): ").strip()

		if not self.re_reply_yes.match(s_reply):
			sys.exit(-1)


	def text_to_binary(self):
		print "Iterating over root dir"
		proc.for_all_odfs_txt(s_root_dir=self.s_root_dir, 
								fn_do=self.proc.convert_txt2bin)		


	def text_to_dynamodb(self):
		print "Iterating over root dir"
		proc.for_all_odfs_txt(s_root_dir=self.s_root_dir,
							fn_do=self.proc.convert_txt2dd)	

	def execute(self):
		""" Execute the commands passed on the command line.
		"""

		if self.b_ask:
			self.prompt_interactive()

		if self.b_binary:
			self.text_to_binary()
		elif self.b_dynamodb:
			self.text_to_dynamodb()

	def read_settings(self, s_settings_file):
		fp_settings = open(s_settings_file, "r")

		s_settings = fp_settings.read()

		fp_settings.close()

		return eval(s_settings)

	def main(self):

		self.d_settings = {}

		try:
			self.d_settings = self.read_settings("settings.txt")
		except:
			log.fatal("Error reading settings.txt.")
			sys.exit(-1)

		args = self.arg_parse()

		self.set_args(args)

		s_aws_access_key_id = self.d_settings["DDACCESSKEY"]
		s_aws_secret_access_key = self.d_settings["DDSECRETACCESSKEY"]
		s_region_name = self.d_settings["DDREGION"]

		self.s_root_dir = self.d_settings["LD_DD_DATA_ROOT"]

		print "Creating DDStore"
		ddstore = DDStore(s_aws_access_key_id,
						s_aws_secret_access_key,
						s_region_name)

		self.ddstore = ddstore

		print "Creating ODFProcessor"
		proc = ODFProcessor(ddstore)

		self.proc = proc

		self.execute()	

if __name__ == "__main__":
	app = TextODFExporter()
	app.main()
