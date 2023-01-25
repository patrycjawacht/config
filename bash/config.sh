#!/bin/bash

set -e
set -u 
set -x

source variables.sh
echo "Initialize new kernel"

KERNEL_NAME="2023_kernel"
REQUIREMENTS="requirements.txt"

VENV_KERNEL_DIR="${HOME}/.venv/env/${KERNEL_NAME}"
#pwd

# virtual environment
pip install virtualenv
virtualenv ${VENV_KERNEL_DIR}
source ${VENV_KERNEL_DIR}/bin/activate

## python3 -m venv .venv/env/${KERNEL_NAME}
## source ${DIRNAME_PATH}/.venv/env/202301_kernel/bin/activate

# install kernel to jupyter
pip install virtualenv ipykernel
python3 -m ipykernel install --user --name ${KERNEL_NAME} --display-name ${KERNEL_NAME}

# install requirements to the kernel
pip install -r ${REQUIREMENTS}