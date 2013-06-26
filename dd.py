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

import os
import os.path
import decimal as dc
import time
import re
import threading
import queue
import glob
import datetime as dt
import shutil

import boto.dynamodb
from boto.dynamodb.item import Item
import boto.dynamodb.condition

from odfexcept import *
import odf
import fifo
import odfproc

# Create logger
import logging
log = logging.getLogger(__name__)

class AbstractDDStore(object):

	def __init__(self):
		pass

	def get_exchange_basename(self, s_exchange):
		return os.path.basename(s_exchange)

	def get_odf_basename(self, s_odf_name):
		s_odf_basename = os.path.basename(s_odf_name)
		s_odf_basename = re.sub(r'\.rs3', '', s_odf_basename)
		return s_odf_basename

	def get_odf_symbol(self, s_odf_basename):
		m = re.match(r'^(.*)\-.*$', s_odf_basename)

		if not m:
			raise ODFException("Invalid ODF Name %s" % s_odf_basename)

		return m.group(1)

	def get_fifo_basename(self, s_fifo_dd):
		s_fifo_basename = os.path.basename(s_fifo_dd)
		s_fifo_basename = re.sub(r'\.fif', '', s_fifo_basename)
		return s_fifo_basename

	def list_exchanges(self):
		pass

	def list_odfs(self, s_exchange):
		pass

	def open_odf(self, s_exchange, s_odf_dd):
		pass

	def save_odf(self, s_odf_dd, s_odf_basename, odf_obj):
		pass

	def get_fifo_dir(self, s_exchange, s_odf_basename):
		pass

	def get_fifo_path(self, s_exchange, s_odf_basename):
		pass

	def fifo_exists(self, s_fifo_dd, s_fifo_basename):
		pass

	def is_fifo_older_than(self, s_fifo_dd, s_fifo_basename, days):
		pass

	def open_fifo(self, s_fifo_dd, s_fifo_basename):
		pass

	def save_fifo(self, s_fifo_dd, s_fifo_basename, fifo_obj):
		pass

class IOWorker(threading.Thread):
	""" Wrapper for IO Worker method. Each class is a single thread of execution.
	"""
	def __init__(self, queue):
		""" Constructor
		@param queue: Imput queue for worker method.
		"""
		super(IOWorker, self).__init__()
		self.queue = queue

	def run(self):
		""" Threading class entry point for each thread. 
		Hands off task to the actual worker method.

		This method expects each input queue entry to have the folowing structure:
		[ <fn_work_method>, [<l_work_method_args>]]
		fn_work_method: method object, the worker method that actually does all the work.
		l_work_method_args: Liost of arguments to pass to fn_work_method
		"""
		while True:
			l_param = self.queue.get()
			fn_work_method = l_param[0]
			l_work_method_args = l_param[1]
			try:
				fn_work_method(*l_work_method_args)
			except:
				log.exception("Failed task")
				pass
			self.queue.task_done()

class IOScheduler(object):
	""" Scheduler class for multi-threaded I/O
	"""
	def __init__(self, num_threads=4):
		""" Constructor
		@param num_threads: No. of threads to create. (default: 4)
		"""
		self.num_threads = num_threads
		self.queue = queue.Queue()
		super(IOScheduler, self).__init__()

	def start_workers(self):
		""" Start the worker threads in daemon mode.
		"""
		for i in range(self.num_threads):
			t = IOWorker(self.queue)
			t.setDaemon(True)
			t.start()

	def wait_for_workers(self):
		""" Wait for all threaded tasks to complete.
		"""
		self.queue.join()

	def queue_request(self, fn_work_method, l_work_method_args, b_block=True, timeout=None):
		""" The requst object is a list where the first item is a method, and the
		second, a list of parameters to the method.
		@param fn_work_method: Worker method object
		@param l_work_method_args: List fo arguments to pass to worker method object
		@param b_block: Block while waiting to ut items on queue (defauilt: True)
		@param timeout: Timeout value if blocking call. (default:None=indefinite)
		"""
		l_req_obj = [fn_work_method, l_work_method_args]
		self.queue.put(l_req_obj, b_block, timeout)
    

class DDStore(AbstractDDStore):
	""" DynamoDB data store class. Methods to create/get and write to DD tables.
	"""
	TABLE_CREATION_WAIT = 2
	TABLE_UPDATE_WAIT = 2
	BATCH_WRITE_SIZE = 25 # Max number of batched items supported by AWS API
	TABLE_WAIT_MAX_RETRIES = 20
	TABLE_UPDATE_MAX_RETRIES = 200
	TABLE_READ_THROUGHPUT = 10
	TABLE_WRITE_THROUGHPUT = 5
	TABLE_WRITE_THROUGHPUT_OPT = 10
	SINGLE_THREADED_WRITE_THROUGHPUT = 70 # 70 wps is empirically determined min throughput.

	def __init__(self, 
				s_aws_access_key_id,
				s_aws_secret_access_key,
				s_region_name,
				read_units=5,
				write_units=10,
				write_units_opt=500,
				num_threads=8):
		self.s_aws_access_key_id = s_aws_access_key_id
		self.s_aws_secret_access_key = s_aws_secret_access_key
		self.s_region_name = s_region_name
		self.read_units = read_units
		self.write_units = write_units
		self.write_units_opt = write_units_opt
		self.num_threads = num_threads
		
		self.d_write_tp_toggle = { self.write_units: self.write_units_opt, 
									self.write_units_opt: self.write_units }

		self.connection = self.get_connection(s_aws_access_key_id=self.s_aws_access_key_id,
												s_aws_secret_access_key=self.s_aws_secret_access_key,
												s_region_name=self.s_region_name)


	def get_connection(self, 
						s_aws_access_key_id,
						s_aws_secret_access_key,
						s_region_name):
		""" Sets up a boto dynamodb connection object using AWS credentials
		@param s_aws_access_key_id: AWS access key id
		@param s_aws_secret_access_key: AWS Secret access key
		@param region: boto region object
		"""
		connection=boto.dynamodb.connect_to_region(s_region_name,
											aws_access_key_id=s_aws_access_key_id,
											aws_secret_access_key=s_aws_secret_access_key)
		return connection

	def wait_for_table(self, dd_table):
		""" Gets a handle to a DynamoDB table.
		@param s_table_name: Table name.
		"""
		log.debug("Waiting for table %s to be active." % dd_table.name)
		while True:
			dd_table.refresh()
			if dd_table.status == "CREATING":
				time.sleep(self.TABLE_CREATION_WAIT)
			elif dd_table.status == "ACTIVE":
				break
		log.debug("Table %s active" % dd_table.name)
		return dd_table

	def create_table(self, s_table_name, table_schema, read_units=None, write_units=None):
		""" Create a table in DynamoDB with the given name and schema.
		@param s_table_name: Name of the table to create
		@param table_schema: hash and range keys for the table, and their types
		@param read_units: Read throughput
		@param write_units: Write throughput
		"""
		if read_units is None:
			read_units = self.read_units

		if write_units is None:
			write_units = self.write_units

		log.debug("Creating DynamoDB Table %s" % s_table_name)
		dd_table = self.connection.create_table(name=s_table_name,
													schema=table_schema,
													read_units=self.read_units,
													write_units=self.write_units_opt)
		self.wait_for_table(dd_table)

		return dd_table

	def get_table(self, s_table_name, 
					s_hash_key_name, hash_key_proto_value, 
					s_range_key_name, range_key_proto_value,
					read_units=None, write_units=None):
		""" Return a handle to the named table, with the given schema. Create if it doesn't exist.
		@param s_table_name: Name of the Table to get/create
		@param hash_key_name: Name of the table hash key
		@param hash_key_proto_value: Data type of the hash key (str or int)
		@param range_key_name: Name of the range key
		@param range_key_proto_value: Data type of the range key (str, int)
		@param read_units: Read throughput
		@param write_units: Write throughput
		"""

		if read_units is None:
			read_units = self.read_units

		if write_units is None:
			write_units = self.write_units

		table_schema = self.connection.create_schema(hash_key_name=s_hash_key_name,
													hash_key_proto_value=hash_key_proto_value,
													range_key_name=s_range_key_name,
													range_key_proto_value=range_key_proto_value)
		dd_table = None

		ls_tables = self.connection.list_tables()

		if s_table_name in ls_tables:
			dd_table = self.connection.table_from_schema(name=s_table_name,
															schema=table_schema)
			dd_table.refresh()
			if dd_table.status == "ACTIVE":
				return dd_table
			elif dd_table.status == "DELETING":
				# Existing table with the same name is being deleted.
				try:
					log.debug("Waiting for table %s to be deleted." % dd_table.name)
					while True:
						time.sleep(self.TABLE_CREATION_WAIT)
						dd_table.refresh()
						if dd_table.status == "ACTIVE":
							# Table is ready to use
							break
				except:
					# Table was deleted, so we can send a create request now
					dd_table = self.create_table(s_table_name, table_schema, 
												read_units, write_units)
					pass
				return dd_table
			elif dd_table.status == "CREATING":
				log.debug("Waiting for table to be created & active.")
				self.wait_for_table(dd_table)
				return dd_table
		else:
			dd_table = self.create_table(s_table_name, table_schema,
											read_units, write_units)

		return dd_table


	def toggle_write_throughput(self, dd_table):
		""" Toggles write throughput between write_units and write_units_opt

		This method should be used after completing the bulk upload on a table.
		It resets the write throughput to the 'normal' i.e. lower long-term
		setting.

		@param dd_table: Table for which to toggle write throughput.
		"""
		status = False
		for i in range(self.TABLE_UPDATE_MAX_RETRIES):
				dd_table.refresh()
				if not dd_table.status == "ACTIVE":
					time.sleep(self.TABLE_UPDATE_WAIT)
					continue	
				break

		new_write_units = self.d_write_tp_toggle[dd_table.write_units]
		
		if dd_table.write_units == new_write_units:
			return True

		dd_table.update_throughput(read_units=dd_table.read_units, write_units=new_write_units)

		status = False
		for i in range(self.TABLE_UPDATE_MAX_RETRIES):
			dd_table.refresh()

			if dd_table.write_units == new_write_units:
				status = True
				break

			time.sleep(self.TABLE_UPDATE_WAIT)

		return status


	def put_records_multi(self, dd_table, ld_table_recs):
		""" Write the given records to the given DynamoDB table.
		@param dd_table: Table to write to
		@param ld_table_recs: List of records in dict form to write.
		"""

		try:
			num_recs = len(ld_table_recs)

			#num_threads = self.num_threads

			# Set no. of threads to the number we need to match the rated
			# write throughput for this table.
			#log.debug("ddwrite=%d" % dd_table.write_units)
			#log.debug("ddsingle=%d" % self.SINGLE_THREADED_WRITE_THROUGHPUT)
			num_threads = (dd_table.write_units // self.SINGLE_THREADED_WRITE_THROUGHPUT) + 1

			# Use only a single thread in cases where the difference between
			# single threaded throughput & rated throughput is not much.
			if num_threads <= 1:
				log.debug("using single thread")
				self.put_records(self.connection, dd_table, ld_table_recs)
			else:
				iosched = IOScheduler(num_threads)

				ld_list_partitions = []
				partition_size = num_recs // num_threads
				num_partitions = (num_recs // partition_size) + 1
				
				#log.debug("num_threads: %d" % num_threads)
				#log.debug("num_recs: %d" % num_recs)
				#log.debug("partition_size: %d" % partition_size)
				#log.debug("num_partitions: %d" % num_partitions)

				for i in range(num_partitions):
					pstart = i * partition_size
					pend = pstart + partition_size

					if pstart == pend:
						break

					if pend > num_recs:
						pend = num_recs

					#log.debug("pstart: %d" % pstart)
					#log.debug("pend: %d" % pend)

					iosched.queue_request(self.put_records, 
										[self.connection, dd_table, ld_table_recs[pstart:pend]])

				log.debug("Starting IO Workers")
				iosched.start_workers()
				iosched.wait_for_workers()
				log.debug("Completed write.")
		except:
			raise
		log.debug("Write successful.")
		

	def put_records(self, connection, dd_table, 
					ld_table_recs):
		""" Batch write the given ODF record items to the given ODF table

		The Boto toolkit uses a max batch size of 25 items -- these are the max.
		number of items we can send on the wire in a single batch write, furthermore,
		there is a 1MB limit on each request.
		Also, DynamoDB will throttle write requests if they exceed the 
		throughput paramaters or the 1MB request limit. This means any writes beyond the
		table's write throughput setting may be DROPPED. 
		This implementation rate limits the writes deliberately to ensure we do not exceed
		the write thrroughput within reasonable limits -- which in turn ensures AWS
		won't drop any writes.

		@param odf_table: Handle to the ODF table
		@param: ld_odf_recs: List of ODF record items, represented as dictionary objects.
		"""
		num_recs = len(ld_table_recs)
		batch_list = connection.new_batch_write_list()
		l_batch_items = []
		debug_frequency = 100
		s_hash_key_name = dd_table.schema.hash_key_name
		s_range_key_name = dd_table.schema.range_key_name
		for i, d_table_rec in enumerate(ld_table_recs):
			# First create an Item type for this ODF record
			ls_attrs = list(d_table_rec.keys())
			ls_attrs.remove(s_hash_key_name)
			ls_attrs.remove(s_range_key_name)

			d_attrs = {}
			for s_attr in ls_attrs:
				d_attrs[s_attr] = d_table_rec[s_attr]

			dd_item = Item(dd_table, 
							hash_key=d_table_rec[s_hash_key_name], 
							range_key=d_table_rec[s_range_key_name],
							attrs=d_attrs)

			l_batch_items.append(dd_item)
			
			# If we've added BATCH_WRITE_SIZE items, flush the batch_list
			if (i+1) % self.BATCH_WRITE_SIZE == 0:
				# Add Item to the current batch
				batch_list.add_batch(dd_table, puts=l_batch_items)
				while True:
					response = connection.batch_write_item(batch_list)
					unprocessed = response.get('UnprocessedItems', None)
					if not unprocessed:
						percent_complete = int(((i+1)/num_recs) * 100)
						if percent_complete > 0 and percent_complete % debug_frequency == 0:
							log.debug("%d records written. %d%% completed." % ((i+1),
																				percent_complete))
						batch_list = connection.new_batch_write_list()
						l_batch_items = []
						break
					# There were unprocessed items. retry only these items
					batch_list = connection.new_batch_write_list()
					unprocessed_list = unprocessed[dd_table.name]
					items = []
					for u in unprocessed_list:
						item_attr = u['PutRequest']['Item']
						item = dd_table.new_item(attrs=item_attr)
						items.append(item)
						batch_list.add_batch(dd_table, puts=items)

	def list_exchanges(self):
		ls_tables = self.connection.list_tables()
		return ls_tables		

	def list_odfs(self, s_exchange):

		odf_table = self.get_table(s_exchange,
									'ODF_NAME',
									str,
									'ODF_RECNO',
									int)

		d_scan_filter = {
			'ODF_RECNO': boto.dynamodb.condition.EQ(1),
		}

		odf_recs = odf_table.scan(scan_filter=d_scan_filter,
									attributes_to_get=['ODF_NAME'],)

		ls_odf_names = []
		for odf_rec in odf_recs:
			ls_odf_names.append(odf_rec['ODF_NAME'])

		return ls_odf_names

	def list_fifos(self, s_exchange):

		s_fifo_table_name = "_".join([s_exchange + "fifo"])

		odf_table = self.get_table(s_fifo_table_name,
									'FIFO_NAME',
									str,
									'FIFO_RECNO',
									int)

		d_scan_filter = {
			'FIFO_RECNO': boto.dynamodb.condition.EQ(1),
		}

		fifo_recs = odf_table.scan(scan_filter=d_scan_filter,
									attributes_to_get=['FIFO_NAME'],)

		ls_fifo_names = []
		for fifo_rec in fifo_recs:
			ls_fifo_names.append(fifo_rec['FIFO_NAME'])

		return ls_fifo_names

	def get_object(self, s_table_name, s_name_key, s_name_value, ls_attribs, d_table_schema):
		dd_table = self.get_table(s_table_name,
									**d_table_schema)

		d_scan_filter = {
			s_name_key : boto.dynamodb.condition.EQ(s_name_value),
		}

		l_dd_recs = dd_table.scan(scan_filter=d_scan_filter,
									attributes_to_get=ls_attribs)

		return l_dd_recs



class LocalDDStore(AbstractDDStore):

	def __init__(self, s_local_dd_data_root):
		self.s_root_dir = os.path.abspath(s_local_dd_data_root)
		self.b_force_fifo_old  = False

	def clear_state(self, s_exchange_basename, s_odf_basename):
		s_fifo_dir = self.get_fifo_dir(s_exchange_basename, 
										s_odf_basename)

		if os.path.exists(s_fifo_dir):
			shutil.rmtree(s_fifo_dir)
		s_txt_odf_name = self.get_text_odf(s_exchange_basename, s_odf_basename)
		proc = odfproc.ODFProcessor(self)	
		proc.convert_txt2bin(s_exchange_basename, s_txt_odf_name, True)

	def list_exchanges(self):
		s_root_glob = os.sep.join([self.s_root_dir, '*'])
		ls_exchanges = glob.glob(s_root_glob)
		return ls_exchanges

	def get_exchange_basename(self, s_exchange):
		return os.path.basename(s_exchange)

	def list_txt_odfs(self, s_exchange):
		s_exchange_glob = os.sep.join([s_exchange, '*.rs4'])
		ls_rs4s = glob.glob(s_exchange_glob)
		return ls_rs4s

	def list_odfs(self, s_exchange):
		s_exchange_glob = os.sep.join([s_exchange, '*.rs3'])
		ls_rs3s = glob.glob(s_exchange_glob)
		return ls_rs3s

	def get_text_odf(self, s_exchange_basename, s_odf_basename):
		s_txt_odf_name = os.sep.join([self.s_root_dir, s_exchange_basename, s_odf_basename])
		s_txt_odf_name = '.'.join([s_txt_odf_name, 'rs4'])
		return s_txt_odf_name
	
	def get_odf_basename(self, s_odf_name):
		s_odf_basename = os.path.basename(s_odf_name)
		s_odf_basename = re.sub(r'\.rs3', '', s_odf_basename)
		return s_odf_basename

	def get_odf_symbol(self, s_odf_basename):
		m = re.match(r'^(.*)\-.*$', s_odf_basename)

		if not m:
			raise ODFException("Invalid ODF Name %s" % s_odf_basename)

		return m.group(1)

	def open_odf(self, s_exchange_basename, s_odf_dd):
		odf_obj = odf.ODF()
		fp_odf_bin = open(s_odf_dd, "rb")
		odf_obj.read_bin_stream(fp_odf_bin)
		fp_odf_bin.close()
		odf_obj.set_store(self)
		return odf_obj

	def save_odf(self, s_odf_dd, s_odf_basename, odf_obj):
		odf_obj.to_bin_file(s_odf_dd)

	def get_fifo_dir(self, s_exchange_basename, s_odf_basename):
		s_fifo_root_dir = os.path.dirname(self.s_root_dir)
		s_fifo_root_dir = os.sep.join([s_fifo_root_dir, 'fifo'])
		s_fifo_dir = os.sep.join([s_fifo_root_dir, s_exchange_basename])
		return s_fifo_dir

	def get_fifo_path(self, s_exchange, s_odf_basename):
		
		s_fifo_dd = '.'.join([s_odf_basename, 'fif'])
		
		s_fifo_dir = self.get_fifo_dir(s_exchange, s_odf_basename)
		
		s_fifo_dd = os.sep.join([s_fifo_dir, s_fifo_dd])
		
		if not os.path.exists(s_fifo_dir):
			os.makedirs(s_fifo_dir)

		return s_fifo_dd

	def get_fifo_basename(self, s_fifo_dd):
		s_fifo_basename = os.path.basename(s_fifo_dd)
		s_fifo_basename = re.sub(r'\.fif', '', s_fifo_basename)
		return s_fifo_basename

	def fifo_exists(self, s_fifo_dd, s_fifo_basename):
		return os.path.exists(s_fifo_dd)

	def is_fifo_older_than(self, s_fifo_dd, s_fifo_basename, days):
		if self.b_force_fifo_old:
			return True
		dt_fifo_mtime = dt.datetime.fromtimestamp(os.path.getmtime(s_fifo_dd))
		dt_now = dt.datetime.now()
		dt_delta = dt_now - dt_fifo_mtime

		if dt_delta > dt.timedelta(days):
			return True

		return False

	def open_fifo(self, s_fifo_dd, s_fifo_basename):
		fifo_obj = fifo.FIFO()
		fp_fifo_bin = open(s_fifo_dd, "rb")
		fifo_obj.read_bin_stream(fp_fifo_bin)
		fp_fifo_bin.close()
		fifo_obj.set_store(self)
		return fifo_obj

	def save_fifo(self, s_fifo_dd, s_fifo_basename, fifo_obj, b_save_csv=False):
		fifo_obj.to_bin_file(s_fifo_dd)
		if b_save_csv:
			fifo_obj.to_csv_file(s_fifo_dd + ".csv")
		

def get_dd_store(config):

	if config.b_test_mode:
		return LocalDDStore(config.s_local_dd_data_root)

	s_aws_access_key_id = config.s_dd_access_key
	s_aws_secret_access_key = config.s_dd_secret_access_key
	s_region_name = config.s_dd_region
	read_units = int(config.dd_read_units)
	write_units = int(config.dd_write_units)
	write_units_opt = int(config.dd_write_units_opt)

	return DDStore(s_aws_access_key_id,
					s_aws_secret_access_key,
					s_region_name,
					read_units,
					write_units,
					write_units_opt)