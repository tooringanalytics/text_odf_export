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
import boto.s3.connection
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
import re


class AbstractFileStore(object):
	lsep = os.sep
	
	def __init__(self):
		pass

	def local_abspath(self, s_path):
		return os.path.abspath(s_path)
	
	def local_dirname(self, s_path):
		return os.path.dirname(s_path)
	
	def local_path_exists(self, s_path):
		return os.path.exists(s_path)
	
	def local_make_dirs(self, s_dir):
		return os.makedirs(s_dir)
		
	def local_rmtree(self, s_path):
		return shutil.rmtree(s_path)
	
	def local_remove(self, s_path):
		return os.remove(s_path)

	def clear_state(self, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
		(s_odf_bucket, s_odf_remote_path) = self.get_odf_remote_path(s_exchange_basename, 
																	s_odf_basename)

		if self.remote_path_exists(s_odf_bucket, s_odf_remote_path):
			self.remote_remove(s_odf_remote_path)

		self.clear_fce_state(s_app_dir, s_exchange_basename, s_symbol, s_odf_basename)

	def clear_fce_state(self, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
		# Delete tmp path, fce output directory on s3 and chunk output directory on s3
		fce_pathspec = self.get_fce_pathspec(s_app_dir, s_exchange_basename, s_symbol, s_odf_basename)

		(s_fce_local_dir, s_fce_local_path) = self.get_fce_local_path(self, fce_pathspec, fce_pathspec.s_fce_header_filename)

		if self.local_path_exists(s_fce_local_path):
			self.local_remove(s_fce_local_path)
			
		(s_fce_bucket, s_fce_remote_path) = self.get_fce_remote_path(self, fce_pathspec, fce_pathspec.s_fce_header_filename)
		
		if self.remote_path_exists(s_fce_bucket, s_fce_remote_path):
			self.remote_remove(s_fce_bucket, s_fce_remote_path)
	
	def get_odf_remote_path(self, s_exchange_basename, s_odf_basename):

		s_odf_remote_path = self.remote_make_path(self.s_bucket, s_exchange_basename, "odf", s_odf_basename)
		s_odf_remote_path = '.'.join([s_odf_remote_path, "rs3"])
		s_odf_bucket = self.remote_bucket(s_odf_remote_path)

		if not self.remote_bucket_exists(s_odf_bucket):
			self.remote_make_bucket(s_odf_bucket)

		return (s_odf_bucket, s_odf_remote_path)

	def save_odf(self, s_exchange_basename, s_odf_basename, odf_obj):
		
		(s_odf_local_dir, s_odf_local_path) = self.get_odf_local_path(s_exchange_basename, s_odf_basename)
		
		if not self.local_path_exists(s_odf_local_dir):
			self.local_make_dirs(s_odf_local_dir)

		odf_obj.to_bin_file(s_odf_local_path)
		
		(s_odf_bucket, s_odf_remote_path) = self.get_odf_remote_path(s_exchange_basename, s_odf_basename)
		
		self.upload_file(s_odf_local_path, s_odf_bucket, s_odf_remote_path)
		
		return True		

	'''
		def get_fce_pathspec(self, s_app_dir,
									s_exchange_basename,
									s_symbol,
									s_odf_basename):
			return context.FCEContext(s_app_dir,
									s_exchange_basename,
									s_symbol,
									s_odf_basename)
	'''

	def get_fce_local_path(self, ctx, s_fce_header_filename):
		s_fce_local_path = self.lsep.join([ctx.s_fce_local_dir, "18", 
											str(ctx.odf_jsunnoon), s_fce_header_filename])
		
		s_fce_local_dir = self.local_dirname(s_fce_local_path)
		return (s_fce_local_dir, s_fce_local_path)
	
	def get_fce_remote_path(self, ctx, s_fce_header_filename):
				
		s_fce_remote_path = self.remote_make_path(self.s_bucket, ctx.s_fce_remote_prefix, "18", 
											str(ctx.odf_jsunnoon), s_fce_header_filename)
		
		s_fce_bucket = self.remote_bucket(s_fce_remote_path)
		#log.debug(s_fce_bucket)
		#log.debug(s_fce_remote_path)
		return (s_fce_bucket, s_fce_remote_path)

	def open_fce(self, ctx, s_fce_header_filename, key=None):

		(s_fce_local_dir, s_fce_local_path) = self.get_fce_local_path(ctx, s_fce_header_filename)

		if not self.local_path_exists(s_fce_local_path):
			(s_fce_bucket, s_fce_remote_path) = self.get_fce_remote_path(ctx, s_fce_header_filename)			
			
			if self.remote_path_exists(s_fce_bucket, s_fce_remote_path):
				if not self.local_path_exists(s_fce_local_dir):
					self.local_make_dirs(s_fce_local_dir)
				self.download_file(s_fce_bucket, s_fce_remote_path, s_fce_local_path)
			else:
				return None
		
		fp_bin = open(s_fce_local_path, "rb")

		fce_obj = fce.FCE()

		fce_obj.read_bin_stream(fp_bin, key)
		
		fp_bin.close()

		return fce_obj

	def fce_exists(self, ctx, s_fce_header_filename):
		
		(s_fce_local_dir, s_fce_local_path) = self.get_fce_local_path(ctx, s_fce_header_filename)
		
		if self.local_path_exists(s_fce_local_path):
			return True

		(s_fce_bucket, s_fce_remote_path) = self.get_fce_remote_path(ctx, s_fce_header_filename)
		
		if self.remote_path_exists(s_fce_bucket, s_fce_remote_path):
			if not self.local_path_exists(s_fce_local_dir):
				self.local_make_dirs(s_fce_local_dir)
			self.download_file(s_fce_bucket, s_fce_remote_path, s_fce_local_path)
			return True

		return False

	def save_fce(self, ctx, s_fce_header_filename, fce_obj, key=None, b_save_csv=False):
		
		# First save the fce in the tmp directory
		(s_fce_local_dir, s_fce_local_path) = self.get_fce_local_path(ctx, s_fce_header_filename)

		if not self.local_path_exists(s_fce_local_dir):
			self.local_make_dirs(s_fce_local_dir)

		fce_obj.to_bin_file(s_fce_local_path, key)
		
		if b_save_csv:
			fce_obj.to_csv_file(s_fce_local_path + ".csv")
		
		# copy the tmp file to S3 directory
		(s_fce_bucket, s_fce_remote_path) = self.get_fce_remote_path(ctx, s_fce_header_filename)
		
		#log.debug(s_fce_bucket)
		#log.debug(s_fce_remote_path)
		self.upload_file(s_fce_local_path, s_fce_bucket, s_fce_remote_path)
		

	def get_chunk_file_local_path(self, ctx, L_no, fce_jsunnoon, s_chunk_file_name):
		
		s_chunk_file_local_path = self.lsep.join([ctx.s_fce_local_dir, str(L_no), str(fce_jsunnoon), s_chunk_file_name])
		
		s_chunk_local_dir = self.local_dirname(s_chunk_file_local_path)
		
		if not self.local_path_exists(s_chunk_local_dir):
			self.local_make_dirs(s_chunk_local_dir)
		
		return (s_chunk_local_dir, s_chunk_file_local_path)

	def get_chunk_file_remote_path(self, ctx, L_no, fce_jsunnoon, s_chunk_file_name):
		
		s_chunk_file_remote_path = self.remote_make_path(self.s_bucket, ctx.s_fce_remote_prefix, str(L_no), str(fce_jsunnoon), s_chunk_file_name)
		
		s_chunk_file_bucket = self.remote_bucket(s_chunk_file_remote_path)
		
		if not self.remote_bucket_exists(s_chunk_file_bucket):
			try:
				self.remote_make_bucket(s_chunk_file_bucket)
			except:
				log.error(s_chunk_file_bucket)
				log.error(s_chunk_file_remote_path)
				raise
			
		return (s_chunk_file_bucket, s_chunk_file_remote_path)

	def chunk_file_exists(self, ctx, L_no, fce_jsunnoon, s_chunk_file_name):
		
		(s_chunk_file_local_dir, s_chunk_file_local_path) = self.get_chunk_file_local_path(ctx, L_no, fce_jsunnoon, s_chunk_file_name)
		
		if self.local_path_exists(s_chunk_file_local_path):
			return True

		(s_chunk_file_bucket, s_chunk_file_remote_path) = self.get_chunk_file_remote_path(ctx, L_no, fce_jsunnoon, s_chunk_file_name)
		
		if self.remote_path_exists(s_chunk_file_bucket, s_chunk_file_remote_path):
			if not self.local_path_exists(s_chunk_file_local_dir):
				self.local_make_dirs(s_chunk_file_local_dir)
			self.download_file(s_chunk_file_bucket, s_chunk_file_remote_path, s_chunk_file_local_path)
			return True

		return False

	def open_chunk_file(self, ctx, L_no, fce_jsunnoon, s_chunk_file_name, chunk_size, key):
		
		(s_chunk_file_local_dir, s_chunk_file_local_path) = self.get_chunk_file_local_path(ctx, L_no, fce_jsunnoon, s_chunk_file_name)
		
		chunk_arr_short = chunk.read_short_chunk_array(s_chunk_file_local_path, s_chunk_file_name, chunk_size, key)
		
		return chunk_arr_short
	
	def save_chunk_file(self, ctx, L_no, fce_jsunnoon, chunk_array, key=None, b_do_csv_chunk=False):
		
		s_chunk_file_name = chunk_array.get_name()
		
		# First save the fce in the tmp directory
		(s_chunk_file_local_dir, s_chunk_file_local_path) = self.get_chunk_file_local_path(ctx, L_no, fce_jsunnoon, s_chunk_file_name)

		if not self.local_path_exists(s_chunk_file_local_dir):
			self.local_make_dirs(s_chunk_file_local_dir)

		chunk_array.to_bin_file_short(s_chunk_file_local_path, key)
		
		if b_do_csv_chunk:
			s_chunk_csv_local_path = '.'.join([s_chunk_file_local_path, "csv"])
			#log.debug("Saving chunk CSV: %s..." % (s_chunk_csv_local_path))
			chunk_array.save_csv(s_chunk_csv_local_path)
			
		# copy the tmp file to S3 directory
		(s_chunk_file_bucket, s_chunk_file_remote_path) = self.get_chunk_file_remote_path(ctx, L_no, fce_jsunnoon, s_chunk_file_name)
		
		self.upload_file(s_chunk_file_local_path, s_chunk_file_bucket, s_chunk_file_remote_path)
		
	def download_file(self, s_bucket, s_remote_file_path, s_local_file_path):
		
		s_local_dir = self.local_dirname(s_local_file_path)
		
		if not self.local_path_exists(s_local_dir):
			self.local_make_dirs(s_local_dir)
		
		self.download(s_bucket, s_remote_file_path, s_local_file_path)

	def upload_file(self, s_local_file_path, s_bucket, s_remote_file_path):
		
		if not self.remote_bucket_exists(s_bucket):
			self.remote_make_bucket(s_bucket)
		
		self.upload(s_local_file_path, s_bucket, s_remote_file_path)

	def move_up(self, s_local_file_path, s_bucket, s_remote_file_path):
		
		self.upload_file(s_local_file_path, s_bucket, s_remote_file_path)
		
		self.local_remove(s_local_file_path)

	def move_down(self, s_bucket, s_remote_file_path, s_local_file_path):
		
		self.download_file(s_bucket, s_remote_file_path, s_local_file_path)
		
		self.remote_remove(s_bucket, s_remote_file_path)


class S3Store(AbstractFileStore):
	
	rsep = "/"

	def __init__(self, s_bucket, s_aws_access_id, s_aws_secret_access_key, s_aws_region):
		super(S3Store, self).__init__()
		self.connection = boto.s3.connection.S3Connection(s_aws_access_id,
														s_aws_secret_access_key)
		self.s_bucket = s_bucket

	def get_key(self, s_bucket, s_path):
		bucket = boto.s3.bucket.Bucket(connection=self.connection, name=s_bucket)
		key = boto.s3.key.Key(bucket)
		key.key = s_path
		return key
	
	def remote_make_path(self, s_bucket, *kargs):
		return self.rsep.join(kargs)
	
	def remote_abspath(self, s_bucket):
		return s_bucket
	
	def remote_path_exists(self, s_bucket, s_path):
		key = self.get_key(s_bucket, s_path)
		return key.exists()
	
	def remote_rmtree(self, s_bucket, s_path):
		self.remote_remove(s_bucket, s_path)

	def remote_remove(self, s_bucket, s_path):
		key = self.get_key(s_bucket, s_path)
		key.delete()
	
	def remote_bucket(self, s_path):
		return self.s_bucket
	
	def remote_bucket_exists(self, s_bucket):
		bucket = boto.s3.bucket.Bucket(connection=self.connection, name=s_bucket)
		if not bucket:
			return False
		return True
		
	def remote_make_bucket(self, s_path):
		pass
	
	def upload(self, s_local_file_path, s_bucket, s_remote_file_path):
		key = self.get_key(s_bucket, s_remote_file_path)
		key.set_contents_from_filename(s_local_file_path)
		return key

	def download(self, s_bucket, s_remote_file_path, s_local_file_path):
		key = self.get_key(s_bucket, s_remote_file_path)
		key.get_contents_to_filename(s_local_file_path)
		return key

	
class LocalS3Store(AbstractFileStore):
	
	rsep = os.sep
	
	def __init__(self, s_remote_root):
		self.s_bucket = self.remote_abspath(s_remote_root)
		super(LocalS3Store, self).__init__()

	def remote_make_path(self, s_bucket, *kargs):
		ls_args = [s_bucket]
		ls_args = ls_args + list(kargs)
		return self.rsep.join(ls_args)
	
	def remote_abspath(self, s_bucket):
		return os.path.abspath(s_bucket)
	
	def remote_path_exists(self, s_bucket, s_path):
		return os.path.exists(s_path)
	
	def remote_rmtree(self, s_path):
		return shutil.rmtree(s_path)

	def remote_remove(self, s_bucket, s_path):
		return os.remove(s_path)
			
	def remote_bucket(self, s_path):
		return os.path.dirname(s_path)
	
	def remote_bucket_exists(self, s_bucket):
		return os.path.exists(s_bucket)
		
	def remote_make_bucket(self, s_path):
		return os.makedirs(s_path)
		
	def download(self, s_remote_bucket, s_remote_file_path, s_local_file_path):
		shutil.copyfile(s_remote_file_path, s_local_file_path)
	
	def upload(self, s_local_file_path, s_remote_bucket, s_remote_file_path):
		shutil.copyfile(s_local_file_path, s_remote_file_path)
		
	
def get_s3_store(config):

	if config.b_test_mode:
		s3store = LocalS3Store(config.s_local_s3_data_root)
		return s3store

	s3store = S3Store(config.s_s3_data_root, config.s_s3_access_key, config.s_s3_secret_access_key, config.s_dd_region)
	return s3store
