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

import decimal as dc
import time
import re

import boto.dynamodb
from boto.dynamodb.item import Item

from odfexcept import *

# Create logger
import logging
log = logging.getLogger(__name__)

class DDStore(object):
	""" DynamoDB data store class. Methods to create/get and write to DD tables.
	"""
	TABLE_CREATION_WAIT = 120
	BATCH_WRITE_SIZE = 25 # Max number of batched items supported by AWS API
	TABLE_WAIT_MAX_RETRIES = 20
	TABLE_READ_THROUGHPUT = 1
	TABLE_WRITE_THROUGHPUT = 2

	def __init__(self, 
				s_aws_access_key_id,
				s_aws_secret_access_key,
				s_region_name,
				read_units=1,
				write_units=2):
		self.s_aws_access_key_id = s_aws_access_key_id
		self.s_aws_secret_access_key = s_aws_secret_access_key
		self.s_region_name = s_region_name
		self.read_units = read_units
		self.write_units = write_units

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

	def wait_for_table(self, s_table_name):
		""" Gets a handle to a DynamoDB table.
		@param s_table_name: Table name.
		"""
		odf_table = None
		while True:
			ls_tables = self.connection.list_tables()
			if s_table_name not in ls_tables:
				log.debug("Waiting %d sec for table %s to be created..." % (self.TABLE_CREATION_WAIT, 
																		s_exchange))
				time.sleep(self.TABLE_CREATION_WAIT)
			else:
				odf_table = self.connection.table_from_schema(name=s_exchange,
															schema=odf_schema)
				odf_table.refresh()
		return odf_table

	def do_create_odf_table(self, s_exchange, odf_schema):
		odf_table = self.connection.create_table(name=s_exchange,
													schema=odf_schema,
													read_units=self.read_units,
													write_units=self.write_units)
		log.debug("Waiting for table to be active.")
		while True:
			odf_table.refresh()
			if odf_table.status == "CREATING":
				time.sleep(2)
			elif odf_table.status == "ACTIVE":
				break
		log.debug("Table active")
		return odf_table

	def create_odf_table(self, s_exchange):
		""" Create and return a handle to the named ODF table.
		@param s_exchange: Name of the exchange (also, name of the table)
		"""

		odf_schema = self.connection.create_schema(hash_key_name='ODF_NAME',
											hash_key_proto_value=str,
											range_key_name='ODF_RECNO',
											range_key_proto_value=int)
		odf_table = None

		ls_tables = self.connection.list_tables()

		if s_exchange in ls_tables:
			odf_table = self.connection.table_from_schema(name=s_exchange,
															schema=odf_schema)
			odf_table.refresh()
			if odf_table.status == "ACTIVE":
				return odf_table
			elif odf_table.status == "DELETING":
				log.debug("Waiting for table to be deleted.")
				#time.sleep(self.TABLE_CREATION_WAIT)
				try:
					while True:
						time.sleep(2)
						odf_table.refresh()
						if odf_table.status == "ACTIVE":
							break
				except:
					odf_table = self.do_create_odf_table(s_exchange, odf_schema)
					pass
				return odf_table
			elif odf_table.status == "CREATING":
				log.debug("Waiting for table to be created & active.")
				while True:
					odf_table.refresh()
					if odf_table.status == "CREATING":
						time.sleep(2)
					elif odf_table.status == "ACTIVE":
						break
				log.debug("Table created and active.")
				return odf_table
		else:
			odf_table = self.do_create_odf_table(s_exchange, odf_schema)

		return odf_table

	def put_odf_records(self, odf_table, ld_odf_recs):
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
		num_recs = len(ld_odf_recs)
		batch_list = self.connection.new_batch_write_list()
		l_odf_items = []
		debug_frequency = 10
		for i, d_odf_rec in enumerate(ld_odf_recs):
			# First create an Item type for this ODF record
			ls_attrs = list(d_odf_rec.keys())
			ls_attrs.remove('ODF_NAME')
			ls_attrs.remove('ODF_RECNO')
			d_attrs = {}
			for s_attr in ls_attrs:
				d_attrs[s_attr] = d_odf_rec[s_attr]

			odf_item = Item(odf_table, 
							hash_key=d_odf_rec['ODF_NAME'], 
							range_key=d_odf_rec['ODF_RECNO'],
							attrs=d_attrs)

			l_odf_items.append(odf_item)
			
			# If we've added BATCH_WRITE_SIZE items, flush the batch_list
			if (i+1) % self.BATCH_WRITE_SIZE == 0:
				# Add Item to the current batch
				batch_list.add_batch(odf_table, puts=l_odf_items)
				while True:
					response = self.connection.batch_write_item(batch_list)
					b_unprocessed = response.get('UnprocessedItems', None)
					if not b_unprocessed:
						percent_complete = int(((i+1)/num_recs) * 100)
						if percent_complete % debug_frequency == 0:
							log.debug("%d records written. %d%% completed." % ((i+1),
																				percent_complete))
						batch_list = self.connection.new_batch_write_list()
						l_odf_items = []
						break
					# There were unprocessed items. retry only these items
					batch_list = self.connection.new_batch_write_list()
					unprocessed_list = unprocessed[odf_table.name]
					items = []
					for u in unprocessed_list:
						item_attr = u['PutRequest']['Item']
						item = odf_table.new_item(attrs=item_attr)
						items.append(item)
						batch_list.add_batch(odf_table, puts=items)



