#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
# created_on: 2023-04-03 10:38

"""Gcs Api."""


__author__ = 'Toran Sahu <toran.sahu@yahoo.com>'
__license__ = 'Distributed under terms of the MIT license'


from google.cloud import storage

PROJECT = "ansible-eg"


def list_blobs(bucket_name: str):
    """Lists all the blobs in the bucket.

    :param bucket_name: Your bucket name
    """

    storage_client = storage.Client(project=PROJECT)
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)
    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        print(blob.name)


def read_blob(bucket_name: str, blob_name: str):
    """Read a blob from GCS using file-like IO.

    :param bucket_name: The ID of your GCS bucket
    :param blob_name: The ID of your new GCS object
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("r") as f:
        print(f.read())


def write_blob(bucket_name: str, blob_name: str):
    """Write a blob from GCS using file-like IO.

    :param bucket_name: The ID of your GCS bucket
    :param blob_name: The ID of your new GCS object
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("w") as f:
        f.write("Hello world")


if __name__ == "__main__":
    bucket_name = "my_data_files"
    list_blobs(bucket_name)
