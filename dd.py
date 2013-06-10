from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex,AllIndex
from boto.dynamodb2.table import Table
import boto.dynamodb2

from boto.dynamodb2.types import STRING, NUMBER
import decimal as dc
import time
import re

from odfexcept import *

# Create logger
import logging
log = logging.getLogger(__name__)

class DDStore(object):

	TABLE_CREATION_WAIT = 120
	BATCH_WRITE_WAIT = 2
	BATCH_WRITE_SIZE = 25
	TABLE_WAIT_MAX_RETRIES = 20
	TABLE_READ_THROUGHPUT = 1
	TABLE_WRITE_THROUGHPUT = 2

	re_res_inuse = re.compile(r'ResourceInUseException')
	re_res_notfound = re.compile(r'ResourceNotFoundException')

	def __init__(self, 
				s_aws_access_key_id,
				s_aws_secret_access_key,
				s_region_name,):
		self.s_aws_access_key_id = s_aws_access_key_id
		self.s_aws_secret_access_key = s_aws_secret_access_key
		self.s_region_name = s_region_name

		self.region = self.get_region_from_name(s_region_name)

		self.connection = self.get_connection(self.s_aws_access_key_id,
												self.s_aws_secret_access_key,
												self.region)

	def get_region_from_name(self, s_region_name):
		regions = boto.dynamodb2.regions()
		region = None
		for reg in boto.dynamodb2.regions():
			if reg.name == s_region_name:
				region = reg
				break
		return region

	def get_connection(self, 
						s_aws_access_key_id,
						s_aws_secret_access_key,
						region):
		connection=DynamoDBConnection(
					aws_access_key_id=s_aws_access_key_id,
					aws_secret_access_key=s_aws_secret_access_key,
					region=region,
					)
		return connection

	def wait_for_table(self, s_table_name):
		odf_table = None
		# wait for the table to be created.
		for i in range(self.TABLE_WAIT_MAX_RETRIES):
			try:
				odf_table = self.get_odf_table(s_table_name)
			except: 
				# Table is still unavailable, continue to wait
				odf_table = None
				log.debug("Waiting %d sec for table %s to be created..." % (self.TABLE_CREATION_WAIT, 
																	s_table_name))
				time.sleep(self.TABLE_CREATION_WAIT)
				continue
			# We were able to 'get' the table, so break.
			break
		if odf_table is None:
			raise ODFDBException("Table %s unavailable" % s_table_name)
		return odf_table

	def create_odf_table(self, s_exchange):
		odf_table = None
		try:
			odf_table = Table.create(s_exchange, 
								schema=[
										HashKey('ODF_NAME', data_type=STRING),
										RangeKey('ODF_RECNO', data_type=NUMBER),
										], 
								throughput={
									'read': self.TABLE_READ_THROUGHPUT,
									'write': self.TABLE_WRITE_THROUGHPUT,
								}, 
								indexes=[
									AllIndex('EverythingIndex', parts=[
										HashKey('ODF_NAME', data_type=NUMBER),
										RangeKey('ODF_RECNO', data_type=NUMBER),
									])
								],
								connection=self.connection,)
			log.debug("Waiting %d sec for table %s to be created..." % (self.TABLE_CREATION_WAIT, 
																	s_exchange))
			time.sleep(self.TABLE_CREATION_WAIT)
		except boto.exception.JSONResponseError as err:
			odf_table = self.wait_for_table(s_exchange)
			pass			
		except:
			raise

		return odf_table

	def get_odf_table(self, s_exchange):

		odf_table = Table(s_exchange, 
					schema=[
							HashKey('ODF_NAME', data_type=STRING),
							RangeKey('ODF_RECNO', data_type=NUMBER),
							], 
					indexes=[
						AllIndex('EverythingIndex', parts=[
							HashKey('ODF_NAME', data_type=NUMBER),
							RangeKey('ODF_RECNO', data_type=NUMBER),
						])
					],
					connection=self.connection,)

		return odf_table
	
	def put_odf_records(self, odf_table, ld_odf_recs):
		num_recs = len(ld_odf_recs)
		log.debug("Writing %d records with batch size %d." % (num_recs, self.BATCH_WRITE_SIZE))
		with odf_table.batch_write() as batch:
			for i, d_odf_rec in enumerate(ld_odf_recs):
		
				batch.put_item(data=d_odf_rec,
								overwrite=True)

				if (i+1) % self.BATCH_WRITE_SIZE == 0:
					pause_sec = self.BATCH_WRITE_SIZE / self.TABLE_WRITE_THROUGHPUT
					percent_complete = float(i+1)/float(num_recs) * 100.0
					log.debug("%d records written. %03.2f%% completed. Pausing %d sec." % ((i+1),
																					percent_complete,
																					pause_sec))
					time.sleep(pause_sec)
				

	def put_odf_record(self, odf_table, d_odf_rec):
		#dc.getcontext().prec = 4
		odf_table.put_item(data=d_odf_rec,
							overwrite=True)


