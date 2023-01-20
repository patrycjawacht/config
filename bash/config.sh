#!/bin/bash

set -e
set -u 
set -x

source variables.sh

echo "Initialize new kernel"


KERNEL_NAME=202301_kernel
DIRNAME=${pwd}

pip install virtualenv

python3 -m venv .venv/env/${KERNEL_NAME}

source ${DIRNAME_PATH}.venv/env/202301_kernel/bin/activate

pip install -r requirements.txt

# #pwd - current path