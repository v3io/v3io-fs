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
import re
from urllib.parse import urlparse


def strip_schema(url):
    if not url:
        return url
    url = str(url)
    if '://' in url:
        url = urlparse(url).path
    return url.replace('\\', '/')  # fix in windows


def split_container(path):
    """Split path container & path

    >>> split_container('/bigdata/path/to/file')
    ['bigdata', 'path/to/file']
    """
    path = str(path)  # Might be pathlib.Path
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
    """Remove optional slashes from the start/end."""
    return s.strip('/')


def _norm(p):
    p = re.sub('/+', '/', p)
    return '/' + p if p[0] != '/' else p


def path_equal(p1, p2):
    """Check that two paths are equal"""
    return _norm(p1) == _norm(p2)
