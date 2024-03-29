
TEXT_ODF_EXPORT
=================

Text File to ODF Export Tool.

Usage:
-------

$ python text_odf_export.py -h
usage: text_odf_export.py [-h] [-a] [-s] [-b] [-d]

Text File to ODF Exporter.

optional arguments:
  -h, --help      show this help message and exit
  -a, --ask       Prompt for Y/N before running.
  -s, --show      Show the name of each ODF as it is being processed.
  -b, --binary    Convert text to packed binary, and store locally.
  -d, --dynamodb  Upload ODF data from text file to AWS DynamoDB.

Output:
--------
- Informational messages to console
- Rotational log files are stored in the logs/ directory

Dependencies:
--------------

- Boto-2.9.5 (should be pre-installed)
	-  (py3kport branch for python 3.3)
	-  (master branch for python 2.7)

Compatibility:
---------------

- python 3.0-3.3.2 
- python 2.7.2-2.7.5


Notes:
--------

- Database writes are currently rate-limited in the code deliberately to
ensure DynamoDB does not drop any writes. To increase throughput, increase
the write throughput for DD tables.

Copyright (C) 2013, Anshuman P.Kanetkar