
from binary import BinaryStruct

class ChunkHeader(BinaryStruct):
	ld_fields = [
			# {'<field_name>' : 'x/c/...' },
			{'LOWEST_LOW' : 'L'},
			{'VOLUME_TICK' : 'H'},
			{'CHUNK_OPEN_RECNO' : 'H'},
			{'CHUNK_CLOSE_RECNO' : 'H'},
		]

	def __init__(self, *kargs, **kwargs):
		super(ChunkHeader, self).__init__(*kargs, **kwargs)

class Chunk(BinaryStruct):

	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'OPEN' : 'd'},
		{'HIGH' : 'd'},
		{'LOW' : 'd'},
		{'CLOSE' : 'd'},
		{'VOLUME' : 'd'},
	]

	def __init__(self, *kargs, **kwargs):
		super(Chunk, self).__init__(*kargs, **kwargs)

class ShortChunk(BinaryStruct):
	ld_fields = [
		# {'<field_name>' : 'x/c/...' },
		{'OPEN' : 'H'},
		{'HIGH' : 'H'},
		{'LOW' : 'H'},
		{'CLOSE' : 'H'},
		{'VOLUME' : 'H'},
	]

	def __init__(self, *kargs, **kwargs):
		super(ShortChunk, self).__init__(*kargs, **kwargs)

class ChunkArray(object):


	def __init__(self, chunk_size):
		self.d_chunk_arr = {}

	def set_field(recno, s_field_name, value):
		if recno not in self.d_chunk_arr:
			self.d_chunk_arr[recno] = {}
		self.d_chunk_arr[recno][s_field_name] = value

	def get_field(recno, s_field_name):
		self.d_chunk_arr[recno][s_field_name]

	def get_header_field(s_hdr_field_name):
		self.d_chunk_arr[self.chunk_size+1][s_hdr_field_name]

	def to_bin_short(self):
		buf = b''

		for recno in range(1, self.chunk_size+1):
			d_fields = self.d_chunk_arr[recno]
			chunk_rec = ShortChunk(d_fields=d_fields)
			buf += chunk_rec.to_bin()

		d_hdr_fields = self.d_chunk_arr[recno + 1]
		chunk_hdr_rec = ChunkHeader(d_fields=d_hdr_fields)
		buf += chunk_hdr_rec.to_bin()

		return buf

	def to_bin_long(self):

		buf = b''

		for recno in range(1, self.chunk_size+1):
			d_fields = self.d_chunk_arr[recno]
			chunk_rec = Chunk(d_fields=d_fields)
			buf += chunk_rec.to_bin()

		d_hdr_fields = self.d_chunk_arr[recno + 1]
		chunk_hdr_rec = ChunkHeader(d_fields=d_hdr_fields)
		buf += chunk_hdr_rec.to_bin()

		return buf


class ChunkArrayList(object):

	def __init__(self):
		self.l_chunk_array = []

	def add_chunk_arr(self, chunk_array):
		self.l_chunk_array.append(chunk_array)

