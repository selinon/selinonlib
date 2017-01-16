#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################
"""
Selinon adapter for Amazon S3 storage
"""

try:
    import boto3
    import botocore
except ImportError:
    raise ImportError("Please install boto3 using `pip3 install boto3` in order to use S3 storage")
from selinon import DataStorage


class S3(DataStorage):
    """
    Amazon S3 storage adapter, for credentials configuration see boto3 library configuration
    https://github.com/boto/boto3
    """
    def __init__(self, bucket, location, endpoint_url=None, use_ssl=None,
                 aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        # AWS access key and access id are handled by Boto - place them to config or use env variables
        super().__init__()
        self._bucket_name = bucket
        self._location = location
        self._s3 = None
        self._use_ssl = use_ssl
        self._endpoint_url = endpoint_url
        self._session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key,
                                              region_name=region_name)

    def is_connected(self):
        return self._s3 is not None

    def connect(self):
        # we need signature version v4 as new AWS regions use this version and we won't be able to connect without this
        self._s3 = self._session.resource('s3', config=botocore.client.Config(signature_version='s3v4'),
                                          use_ssl=self._use_ssl, endpoint_url=self._endpoint_url)

        # check that the bucket exists - see boto docs
        try:
            self._s3.meta.client.head_bucket(Bucket=self._bucket_name)
        except botocore.exceptions.ClientError as exc:
            # if a client error is thrown, then check that it was a 404 error.
            # if it was a 404 error, then the bucket does not exist.
            error_code = int(exc.response['Error']['Code'])
            if error_code == 404:
                self._s3.create_bucket(Bucket=self._bucket_name,
                                       CreateBucketConfiguration={
                                           'LocationConstraint': self._location
                                       })
            else:
                raise

    def disconnect(self):
        if self._s3:
            del self._s3
            self._s3 = None

    def retrieve(self, flow_name, task_name, task_id):
        assert self.is_connected()
        return self._s3.Object(self._bucket_name, task_id).get()['Body'].read()

    def store(self, node_args, flow_name, task_name, task_id, result):
        assert self.is_connected()
        self._s3.Object(self._bucket_name, task_id).put(Body=result)
