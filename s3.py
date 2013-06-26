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


import boto.s3
import boto.s3.bucket
import boto.s3.key

import fce
import odf
import chunk

import logging
log = logging.getLogger(__name__)

import os.path
import os
import shutil

class AbstractS3Store(object):

	def __init__(self):
		pass


class S3Store(object):
	sep = "/"
	
	def __init__(self, s_root_dir, s_aws_access_id, s_aws_secret_access_key, s_aws_region):
		super(S3Store, self).__init__()
		self.connection = boto.s3.connection.S3Connection(s_aws_access_id,
														s_aws_secret_access_key)
		self.s_root_dir = s_root_dir

	def upload_s3(self, s_local_file, s_bucket, s_remote_file):
		bucket = boto.s3.bucket.Bucket(connection=self.connection, name=s_bucket)
		key = boto.s3.key.Key(bucket)
		key.key = s_remote_file
		key.set_contents_from_filename(s_local_file)
		return key

	def download_s3(self, s_bucket, s_remote_file, s_local_file):
		bucket = boto.s3.bucket.Bucket(connection=self.connection, name=s_bucket)
		key = boto.s3.key.Key(bucket)
		key.key = s_remote_file
		key.get_contents_to_filename(s_local_file)
		return key

	def move_up_s3(self, s_local_file, s_bucket, s_remote_file):
		self.upload(s_local_file, s_bucket, s_remote_file)
		os.remove(s_local_file)

	def move_down_s3(self, s_bucket, s_remote_file, s_local_file):
		key = self.download(s_bucket, s_remote_file, s_local_file)
		key.delete()

	def exists_s3(self, s_bucket, s_remote_file):
		bucket = boto.s3.bucket.Bucket(connection=self.connection, name=s_bucket)
		key = boto.s3.key.Key(bucket)
		key.key = s_remote_file
		return key.exists()
	
class LocalS3Store(object):

	def __init__(self, s_local_s3_data_root):
		self.s_root_dir = os.path.abspath(s_local_s3_data_root)
		super(LocalS3Store, self).__init__()

	def clear_state(self, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
		s_odf_s3_dir = self.get_odf_dir(s_exchange_basename, 
										s_odf_basename)

		if os.path.exists(s_odf_s3_dir):
			shutil.rmtree(s_odf_s3_dir)

		self.clear_fce_state(s_app_dir, s_exchange_basename, s_symbol, s_odf_basename)


	def clear_fce_state(self, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
		# Delete tmp path, fce output directory on s3 and chunk output directory on s3
		fce_pathspec = self.get_fce_pathspec(s_app_dir, s_exchange_basename, s_symbol, s_odf_basename)

		s_fce_tmp_path = fce_pathspec.s_fce_tmp

		if os.path.exists(s_fce_tmp_path):
			shutil.rmtree(s_fce_tmp_path)

		s_fce_s3_dir = os.sep.join([self.s_root_dir, fce_pathspec.s_fce_s3_prefix])

		if os.path.exists(s_fce_s3_dir):
			shutil.rmtree(s_fce_s3_dir)


	def get_odf_dir(self, s_exchange_basename, s_odf_basename):

		s_odf_s3_path = self.get_odf_path(s_exchange_basename, s_odf_basename)

		return os.path.dirname(s_odf_s3_path)

	def get_odf_path(self, s_exchange_basename, s_odf_basename):
		s_odf_s3 = ""

		s_odf_s3 = os.sep.join([self.s_root_dir, s_exchange_basename, "odf", s_odf_basename])
		s_odf_s3 = '.'.join([s_odf_s3, "rs3"])
		s_odf_s3_dir = os.path.dirname(s_odf_s3)

		if not os.path.exists(s_odf_s3_dir):
			os.makedirs(s_odf_s3_dir)

		return s_odf_s3

	def save_odf(self, s_odf_s3, odf_obj):
		odf_obj.to_bin_file(s_odf_s3)		

	def get_fce_pathspec(self, s_app_dir,
								s_exchange_basename,
								s_symbol,
								s_odf_basename):
		return fce.FCEPathSpec(s_app_dir,
								s_exchange_basename,
								s_symbol,
								s_odf_basename)

	def get_fce_tmp_path(self, fce_pathspec, s_fce_header_filename):
		s_fce_tmp_filename = os.sep.join([fce_pathspec.s_fce_tmp, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		
		s_fce_tmp_dir = os.path.dirname(s_fce_tmp_filename)
		return (s_fce_tmp_dir, s_fce_tmp_filename)

	def get_fce_path(self, fce_pathspec, s_fce_header_filename):		
		s_fce_s3_filename = os.sep.join([self.s_root_dir, fce_pathspec.s_fce_s3_prefix, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		s_fce_s3_dirname = os.path.dirname(s_fce_s3_filename)

		return (s_fce_s3_dirname, s_fce_s3_filename)

	def open_fce(self, fce_pathspec, s_fce_header_filename, key=None):

		(s_fce_tmp_dir, s_fce_tmp_filename) = self.get_fce_tmp_path(fce_pathspec, s_fce_header_filename)

		if not os.path.exists(s_fce_tmp_filename):
			(s_fce_s3_dirname, s_fce_s3_filename) = self.get_fce_path(fce_pathspec, s_fce_header_filename)			
			
			if os.path.exists(s_fce_s3_filename):
				if not os.path.exists(s_fce_tmp_dir):
					os.makedirs(s_fce_tmp_dir)
				self.download_file(s_fce_s3_dirname, s_fce_s3_filename, s_fce_tmp_filename)
			else:
				return None
		
		fp_bin = open(s_fce_tmp_filename, "rb")

		fce_obj = fce.FCE()

		fce_obj.read_bin_stream(fp_bin, key)
		
		fp_bin.close()

		return fce_obj

	def fce_exists(self, fce_pathspec, s_fce_header_filename):
		s_fce_tmp_filename = os.sep.join([fce_pathspec.s_fce_tmp, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		
		if os.path.exists(s_fce_tmp_filename):
			return True

		s_fce_s3_filename = os.sep.join([self.s_root_dir, fce_pathspec.s_fce_s3_prefix, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		
		if os.path.exists(s_fce_s3_filename):
			shutil.copyfile(s_fce_s3_filename, s_fce_tmp_filename)
			return True

		return False

	def save_fce(self, fce_pathspec, s_fce_header_filename, fce_obj, key=None):
		
		# First save the fce in the tmp directory
		s_fce_tmp_filename = os.sep.join([fce_pathspec.s_fce_tmp, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		
		s_fce_tmp_dir = os.path.dirname(s_fce_tmp_filename)

		if not os.path.exists(s_fce_tmp_dir):
			os.makedirs(s_fce_tmp_dir)

		fce_obj.to_bin_file(s_fce_tmp_filename, key)

		# copy the tmp file to S3 directory
		s_fce_s3_filename = os.sep.join([self.s_root_dir, fce_pathspec.s_fce_s3_prefix, "18", 
											str(fce_pathspec.odf_jsunnoon), s_fce_header_filename])
		
		s_fce_s3_dir = os.path.dirname(s_fce_s3_filename)

		self.upload_file(s_fce_tmp_filename, s_fce_s3_dir, s_fce_s3_filename)
		

	def get_chunk_file_tmp_path(self, fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name):
		s_chunk_file_tmp_path = os.sep.join([fce_pathspec.s_fce_tmp, str(L_no), str(fce_jsunnoon), os.path.basename(s_chunk_file_name)])
		s_chunk_tmp_dir = os.path.dirname(s_chunk_file_tmp_path)
		if not os.path.exists(s_chunk_tmp_dir):
			os.makedirs(s_chunk_tmp_dir)
		return (s_chunk_tmp_dir, s_chunk_file_tmp_path)

	def get_chunk_file_path(self, fce_pathspec, L_no, fce_jsunnoon, s_chunk_file_name):
		s_chunk_file_path = os.sep.join([self.s_root_dir, fce_pathspec.s_fce_s3_prefix, str(L_no), str(fce_jsunnoon), os.path.basename(s_chunk_file_name)])
		s_chunk_file_dir = os.path.dirname(s_chunk_file_path)
		if not os.path.exists(s_chunk_file_dir):
			try:
				os.makedirs(s_chunk_file_dir)
			except:
				log.error(s_chunk_file_dir)
				log.error(s_chunk_file_path)
				raise
		return (s_chunk_file_dir, s_chunk_file_path)

	def chunk_file_exists(self, s_chunk_file_dir_s3, s_chunk_file_name_s3):
		return os.path.exists(s_chunk_file_name_s3)

	def save_chunk_file(self, s_chunk_file_name, chunk_array, key=None):
		fp_chunk = open(s_chunk_file_name, "wb")
		buf = chunk_array.to_bin_short(key)
		fp_chunk.write(buf)
		fp_chunk.close()

	def download_file(self, s_s3_bucket, s_s3_file_name, s_local_file_name):
		s_local_dir = os.path.dirname(s_local_file_name)
		if not os.path.exists(s_local_dir):
			os.makedirs(s_local_dir)
		shutil.copyfile(s_s3_file_name, s_local_file_name)

	def upload_file(self, s_local_file_name, s_s3_bucket, s_s3_file_name):
		if not os.path.exists(s_s3_bucket):
			os.makedirs(s_s3_bucket)
		shutil.copyfile(s_local_file_name, s_s3_file_name)

	def move_up(self, s_local_file, s_bucket, s_remote_file):
		shutil.copyfile(s_local_file, s_remote_file)
		os.remove(s_local_file)

def get_s3_store(config):

	if config.b_test_mode:
		s3store = LocalS3Store(config.s_local_s3_data_root)
		return s3store

	s3store = S3Store(config.s_s3_access_key, config.s_s3_secret_access_key, config.s_dd_region)
	return s3store
