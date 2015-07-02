This file is used to resize multiple whisper(part of graphite/carbon) files at once, based on
your storage-schema.conf and storage-aggregations.conf files

Ensure that you are pointing to the correct locations for these files, by setting GRAPHITE_ROOT
environment variable to directory where graphite was installed (typically /opt/graphite)

This script has only been tested on python >= 2.7 and graphite 0.9.10

License:  Apache Software License 2.0 (to match the carbon license https://pypi.python.org/pypi/carbon)
