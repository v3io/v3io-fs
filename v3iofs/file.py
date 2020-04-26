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

import ssl
from urllib.request import Request, urlopen

from fsspec.spec import AbstractBufferedFile

# Ignore certs
no_validate_ctx = ssl.create_default_context()
no_validate_ctx.check_hostname = False
no_validate_ctx.verify_mode = ssl.CERT_NONE

# v3io-py don't support range requests so we're using urllib
# (https://github.com/v3io/v3io-py/issues/16)


class V3ioFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        req = Request(
            url=f'{self.fs._v3io_api}/{self.path}',
            headers={
                'Range': f'bytes={start}-{end}',
                'X-v3io-session-key': self.fs._v3io_access_key,
            },
        )
        with urlopen(req, context=no_validate_ctx) as fp:
            return fp.read()

    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        data = self.buffer.getvalue()
        if not data:
            return
        req = Request(
            method='POST',
            data=data,
            url=f'{self.fs._v3io_api}/{self.path}',
            headers={
                'Range': '-1',
                'Content-Length': len(data),
                'X-v3io-session-key': self.fs._v3io_access_key,
            },
        )
        urlopen(req, context=no_validate_ctx)
