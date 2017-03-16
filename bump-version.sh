#!/bin/sh

echo "Generating new version identifier..."

VERSION=`git describe --tags 2>/dev/null || git rev-parse --short HEAD`
echo -e "selinonlib_version = '${VERSION}'" > selinonlib/version.py

echo "Selinonlib version is '${VERSION}'"
