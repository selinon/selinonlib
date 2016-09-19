#!/bin/sh

echo "Generating new version identifier..."

VERSION=`git describe 2>/dev/null || git rev-parse --short HEAD`
echo -e "selinonlib_version = '${VERSION}'\n" > selinonlib/version.py

echo "Selinonlib version is '${VERSION}'"
