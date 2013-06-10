
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

- Boto-2.9.5 (as provided with this program.)
- python 2.7.2-2.7.5 (for both binary conversion and DynamoDB updates)
- python 3.0-3.3.2 (only for binary conversion)

Notes:
--------

- Database writes are currently rate-limited in the code, deliberately to
ensure DynamoDB does not drop any writes.

Copyright (C) 2013, Anshuman P.Kanetkar