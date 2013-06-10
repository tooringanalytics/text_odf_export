
# Required by argument processor
import sys
import os.path
import re
import glob
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


def main():

	s_root_dir = sys.argv[1]

	s_aws_access_key_id = "AKIAIYSTJTMFXKIGGGVQ"
	s_aws_secret_access_key = "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"
	s_region_name = "us-east-1"

	print "Creating DDStore"
	ddstore = DDStore(s_aws_access_key_id,
						s_aws_secret_access_key,
						s_region_name)

	print "Creating ODFProcessor"
	proc = ODFProcessor(ddstore)

	print "Iterating over root dir"
	proc.for_all_odfs_txt(s_root_dir=s_root_dir, 
							#fn_do=proc.convert_txt2bin)	
						fn_do=proc.convert_txt2dd)	
	
if __name__ == "__main__":
	main()
