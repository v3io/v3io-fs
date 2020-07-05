# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from http import HTTPStatus
import io

from fsspec.spec import AbstractBufferedFile
from fsspec.core import caches
from v3io.dataplane import Client

from .path import split_container


class V3ioFile(AbstractBufferedFile):
    """ File-like operation on V3IO Files """
    
    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
        ):
        
        self.path = path
        self.client = fs._client

        super().__init__(
            fs=fs,
            path=path,
            block_size=block_size,
            mode=mode,
            autocommit=True,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )

    def _fetch_range(self, start, end):
        container, path = split_container(self.path)
        nbytes = end - start

        resp = self.client.get_object(
            container, path, offset=start, num_bytes=nbytes,
            raise_for_status=[HTTPStatus.PARTIAL_CONTENT, HTTPStatus.OK])
        return resp.body

    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        body = self.buffer.getvalue()
        if not body:
            return

        container, path = split_container(self.path)
        self.client.put_object(
            container, path, body=body, append=True,
            raise_for_status=[HTTPStatus.OK],
        )

        # No need to clear self.buffer, fsspec does that
        return True

    def _initiate_upload(self, **kwargs):
        self.client = self.fs._client
        return super()._initiate_upload()
