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


def split_container(path):
    """Split path container & path

    >>> split_container('/bigdata/path/to/file')
    ('bigdata', 'path/to/file')
    """
    if not path:
        raise ValueError('empty path')

    if path == '/':
        return '', ''

    if path[0] == '/':
        path = path[1:]

    if '/' not in path:
        return path, ''  # container

    return path.split('/', maxsplit=1)


def unslash(s):
    """Remove optional slash from the end."""
    if not s or s[-1] != '/':
        return s
    return s[:-1]
