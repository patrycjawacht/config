#!/bin/bash

set -e
set -u
set -x

# choose correct python
alias python3=/usr/bin/python3

# configure key variables
BASH_DIR=$(dirname $(readlink -fn "${0}"))
ROOT_DIR=$(dirname ${BASH_DIR})
PY_DIR=${ROOT_DIR}/python

# can be replaced by scripts if required:
papermill ../python/n1.ipynb ../python/n1.ipynb
papermill ../python/n2.ipynb ../python/n2.ipynb
papermill ../python/n3.ipynb ../python/n3.ipynb
papermill ../python/n4.ipynb ../python/n4.ipynb
# papermill ../python/n5.ipynb ../python/n5.ipynb


# autofulfill
gsutil cp gs://xxxx/autofull-main.zip ${BASH_DIR}
echo "All no" | unzip ${BASH_DIR}/autofull-main.zip

pip install -r ${BASH_DIR}/autofull-main/requirements.txt
pip install ${BASH_DIR}/autofull-main/ds/autofull_upload-1.1.3-py3-none-any.whl


# run the autofulfillment process, no input parameters required
python3 ../python/run_autofull_doc.py 