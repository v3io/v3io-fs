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

__version__ = '0.1.6'

import fsspec

from .file import V3ioFile  # noqa: F401
from .fs import V3ioFS  # noqa: F401

if hasattr(fsspec, 'register_implementation'):
    # TODO: Not sure about clobber=True
    fsspec.register_implementation('v3io', V3ioFS, clobber=True)
else:
    from fsspec.registry import known_implementations
    known_implementations['v3io'] = {
        'class': 'v3iofs.V3ioFS',
        'err': 'Please install v3iofs to use the v3io fileysstem class'
    }

    del known_implementations

del fsspec  # clear the module namespace
