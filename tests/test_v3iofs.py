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

import fsspec
from v3iofs import V3ioFS


def test_register():
    cls = fsspec.get_filesystem_class(V3ioFS.protocol)
    assert cls is V3ioFS, 'not registered'

    fs = fsspec.filesystem('v3io')
    assert isinstance(fs, V3ioFS), f'bad object class - {fs.__class__}'
