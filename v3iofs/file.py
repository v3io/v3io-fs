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


from fsspec.spec import AbstractBufferedFile
from v3io.dataplane import Client
from .utils import handle_v3io_errors
import v3io
from .path import split_container


class V3ioFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        client: Client = self.fs._client
        container, path = split_container(self.path)
        nbytes = end - start

        resp = client.get_object(
            container, path, offset=start, num_bytes=nbytes,
            raise_for_status=v3io.dataplane.RaiseForStatus.never)

        return handle_v3io_errors(resp, path)

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

        client: Client = self.fs._client
        container, path = split_container(self.path)
        resp = client.put_object(
            container, path, body=body, append=True,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
        )

        if handle_v3io_errors(resp, path):
            # No need to clear self.buffer, fsspec does that
            return True

    def _initiate_upload(self):
        """ Create remote file/upload """
        client: Client = self.fs._client
        container, path = split_container(self.path)
        client.object.delete(
            container=container,
            path=path,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
        )
