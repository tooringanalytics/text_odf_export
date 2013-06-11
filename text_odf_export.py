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

import sys
import os.path

# Add the file's parent directory to the package search path.
_lib_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(_lib_path)

# Library imports
import re
import glob
import argparse

# Local imports
from odf import ODF
from odfproc import ODFProcessor

# Create logger
import logging
import logging.handlers
log = logging.getLogger(__name__)


class TextODFExporter(object):
	""" Application class for text_odf_export
	"""

	re_reply_yes = re.compile(r'^(y|(yes))$', re.I)

	def __init__(self):
		""" Constructor
		"""
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
		""" Prompt the user on stdin to continue with the program.
		"""
		s_reply = ""
		
		if self.b_binary:
			sys.stdout.write("This program will generate binary ODFs. Proceed? (y/N): ")
		elif self.b_dynamodb:
			sys.stdout.write("This program will upload ODF data to DynamoDB. Proceed? (y/N): ")

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


	def text_to_binary(self):
		""" Convert the text csv's in the root directory to binary ODFs on local storage.
		"""
		log.debug("Creating ODFProcessor")
		proc = ODFProcessor(ddstore=None)

		proc.for_all_odfs_txt(s_root_dir=self.s_root_dir, 
								fn_do=proc.convert_txt2bin,
								b_show=self.b_show)		


	def text_to_dynamodb(self):
		""" Upload the text csv's in the root dir. to DD tables.
		"""
		s_aws_access_key_id = self.d_settings["DDACCESSKEY"]
		s_aws_secret_access_key = self.d_settings["DDSECRETACCESSKEY"]
		s_region_name = self.d_settings["DDREGION"]
		read_units = int(self.d_settings["DDREADUNITS"])
		write_units = int(self.d_settings["DDWRITEUNITS"])

		log.debug("Creating DDStore")
		from dd import DDStore
		ddstore = DDStore(s_aws_access_key_id,
							s_aws_secret_access_key,
							s_region_name,
							read_units,
							write_units)

		log.debug("Creating ODFProcessor")
		proc = ODFProcessor(ddstore)

		proc.for_all_odfs_txt(s_root_dir=self.s_root_dir,
								fn_do=proc.convert_txt2dd,
								b_show=self.b_show)	

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
		""" Load settings from the settings file
		@param s_settings_file: Path to the settings file.
		"""
		fp_settings = open(s_settings_file, "r")

		s_settings = fp_settings.read()

		fp_settings.close()

		return eval(s_settings)

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

		ls_modules = ['binary', 'odf', 'odfproc', 'odfexcept', 'dd', '__main__']

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
			self.d_settings = self.read_settings("settings.txt")
		except:
			log.fatal("Error reading settings.txt.")
			sys.exit(-1)

		try:
			args = self.arg_parse()

			self.set_args(args)

			self.s_root_dir = self.d_settings["LD_DD_DATA_ROOT"]

			

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

if __name__ == "__main__":
	app = TextODFExporter()
	app.main()
