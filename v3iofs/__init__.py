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
"""v3iofs - An fsspec driver for v3io"""

__all__ = [
    '__version__',
    'V3ioFS',
    'V3ioFile',
]

from fsspec.registry import known_implementations

from .file import V3ioFile  # noqa: F401
from .fs import V3ioFS  # noqa: F401

__version__ = '0.1.0a1'

# Register v3io protocol
# (https://github.com/intake/filesystem_spec/issues/293)
if V3ioFS.protocol not in known_implementations:
    known_implementations[V3ioFS.protocol] = {
        'class': 'v3iofs.V3ioFS',
    }

del known_implementations  # clear the module namespace
