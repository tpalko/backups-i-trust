#!/bin/bash 

set -e

echo "Making ${VENDOR_FOLDER}"
rm -rvf ${VENDOR_FOLDER} && mkdir -vp ${VENDOR_FOLDER}/cowpy

if [[ -n "${COWPY_LOCAL_PATH}" && -d ${COWPY_LOCAL_PATH} ]]; then 
  cp -R ${COWPY_LOCAL_PATH} ${VENDOR_FOLDER}
  ln -svf ${COWPY_LOCAL_PATH}/src/cowpy ${VIRTUALENV_PACKAGES_FOLDER}/
else
  echo "Cloning cowpy repository ${COWPY_REPO}"
  git clone ${COWPY_REPO} --branch develop ${VENDOR_FOLDER}/cowpy
  ln -svf ${VENDOR_FOLDER}/cowpy/src/cowpy ${VIRTUALENV_PACKAGES_FOLDER}/
fi 

if [[ ! -d ${VENDOR_FOLDER}/cowpy/src/cowpy ]]; then 
  echo "cowpy src folder was not found, skipping virtualenv symlink"
  exit 1
fi 

if [[ ! -d ${VIRTUALENV_PACKAGES_FOLDER} ]]; then 
  echo "virtualenv folder was not found, skipping virtualenv symlink"
  exit 1
fi 


