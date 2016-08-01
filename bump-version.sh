#!/bin/sh

echo "Generating new version identifier..."

VERSION=`git describe 2>/dev/null || git rev-parse --short HEAD`
echo -e "parsley_version = '${VERSION}'\n" > parsley/version.py

echo "Parsley version is '${VERSION}'"
