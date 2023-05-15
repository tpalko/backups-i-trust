#!/bin/bash 

set -e

echo "Making ${VENDOR_FOLDER}"
rm -rvf ${VENDOR_FOLDER} && mkdir -vp ${VENDOR_FOLDER}/cowpy

# we have a vendor folder here for packaging and a virtualenv for dev runtime 

echo "Testing ${COWPY_LOCAL_PATH}"

# -- if we have source on the host, copy it to vendor for packaging
# -- and symlink directly to virtualenv
if [[ -n "${COWPY_LOCAL_PATH}" && -d ${COWPY_LOCAL_PATH} ]]; then 
  echo "Copying local cowpy source for packaging from ${COWPY_LOCAL_PATH}"
  cp -Rv ${COWPY_LOCAL_PATH} ${VENDOR_FOLDER}
  echo "Linking ${COWPY_LOCAL_PATH}/src/cowpy -> ${VIRTUALENV_PACKAGES_FOLDER}/"
  ln -svf ${COWPY_LOCAL_PATH}/src/cowpy ${VIRTUALENV_PACKAGES_FOLDER}/
else
  # -- otherwise, clone it into vendor
  # -- and symlink from there for runtime 
  echo "Cloning cowpy repository ${COWPY_REPO}"
  git clone ${COWPY_REPO} --branch develop ${VENDOR_FOLDER}/cowpy
  ln -svf ${VENDOR_FOLDER}/cowpy/src/cowpy ${VIRTUALENV_PACKAGES_FOLDER}/
fi 

if [[ ! -d ${VENDOR_FOLDER}/cowpy/src/cowpy ]]; then 
  echo "cowpy src folder was not found at ${VENDOR_FOLDER}/cowpy/src/cowpy, skipping virtualenv symlink"
  exit 1
fi 

if [[ ! -d ${VIRTUALENV_PACKAGES_FOLDER} ]]; then 
  echo "virtualenv folder was not found, skipping virtualenv symlink"
  exit 1
fi 


