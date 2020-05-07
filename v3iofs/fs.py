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

from datetime import datetime, timezone
from http import HTTPStatus
from os import environ
from urllib.parse import urlparse

from fsspec.spec import AbstractFileSystem
from v3io.dataplane import Client

from .path import split_container, unslash


class V3ioFS(AbstractFileSystem):
    """File system driver to v3io

    Parameters
    ----------
    v3io_api: str
        API host name (or V3IO_API environment)
    v3io_access_key: str
        v3io access key (or V3IO_ACCESS_KEY from environment)
    **kw:
        Passed to fsspec.AbstractFileSystem
    """

    protocol = 'v3io'

    def __init__(self, v3io_api=None, v3io_access_key=None, **kw):
        # TODO: Support storage options for creds (in kw)
        super().__init__(**kw)
        self._v3io_api, self._v3io_access_key = \
            self._parse_api(v3io_api, v3io_access_key)
        self._client = Client(
            endpoint=self._v3io_api,
            access_key=self._v3io_access_key,
            transport_kind='requests',
        )

    def ls(self, path, detail=True, **kwargs):
        """Lists files & directories under path"""
        container, path = split_container(path)

        if not container:
            return self._list_containers(detail)

        resp = self._client.get_container_contents(
            container=container,
            path=path,
            get_all_attributes=True,
            raise_for_status=[HTTPStatus.OK],
        )

        prefixes = resp.output.common_prefixes  # directories
        fn = prefix_info if detail else prefix_path
        dirs = [fn(container, p) for p in prefixes]

        objects = resp.output.contents  # files
        fn = object_info if detail else object_path
        files = [fn(container, o) for o in objects]

        return dirs + files

    def _parse_api(self, v3io_api, v3io_access_key):
        v3io_api = v3io_api or environ.get('V3IO_API')
        url, auth = split_auth(v3io_api)
        if auth:
            return url, auth

        v3io_access_key = v3io_access_key or environ.get('V3IO_ACCESS_KEY')
        return v3io_api, v3io_access_key

    def _list_containers(self, detail):
        resp = self._client.get_containers(
            raise_for_status=[HTTPStatus.OK],
        )
        fn = container_info if detail else container_path
        return [fn(c) for c in resp.output.containers]

    def copy(self, path1, path2, **kwargs):
        ...  # FIXME

    def _rm(self, path):
        container, path = split_container(path)
        if not container:
            raise ValueError(f'bad path: {path:r}')

        self._client.delete_object(
            container=container,
            path=path,
            raise_for_status=[HTTPStatus.NO_CONTENT],
        )

    def touch(self, path, truncate=True, **kwargs):
        if not truncate:  # TODO
            raise ValueError('only truncate touch supported')
        container, path = split_container(path)
        self._client.put_object(
            container, path,
            raise_for_status=[HTTPStatus.OK],
        )


def container_path(container):
    return f'/{container.name}'


def container_info(container):
    return {
        'name': container.name,
        'size': None,
        'created': parse_time(container.creation_date),
    }


def prefix_path(container_name, prefix):
    if not isinstance(prefix, str):
        prefix = prefix.prefix
    # prefix already have a leading /
    return unslash(f'/{container_name}{prefix}')


def prefix_info(container_name, prefix):
    return info_of(container_name, prefix, 'prefix')


def object_path(container_name, object):
    # object.key already have a leading /
    return f'/{container_name}{object.key}'


def object_info(container_name, object):
    return info_of(container_name, object, 'key')


def info_of(container_name, obj, name_key):
    return {
        'name': prefix_path(container_name, getattr(obj, name_key)),
        'size': getattr(obj, 'size', None),
        'created': parse_time(obj.creating_time),
        'mtime': parse_time(obj.last_modified),
        'atime': parse_time(obj.access_time),
        'mode': int(obj.mode[1:], base=8),  # '040755'
        'gid': int(obj.gid, 16),
        'uid': int(obj.uid, 16),
    }


def parse_time(creation_date):
    # '2020-03-26T09:42:57.504000+00:00'
    # '2020-03-26T09:42:57.71Z'
    i = creation_date.rfind('+')  # If not found will be -1, good for Z
    dt = datetime.strptime(creation_date[:i], '%Y-%m-%dT%H:%M:%S.%f')
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def split_auth(url):
    """
    >>> split_auth('v3io://api_key:s3cr3t@domain.company.com')
    ('v3io://domain.company.com', 's3cr3t')
    >>> split_auth('v3io://domain.company.com')
    ('v3io://domain.company.com', '')
    """
    u = urlparse(url)
    if '@' not in u.netloc:
        return (url, '')

    auth, netloc = u.netloc.split('@', 1)
    if ':' not in auth:
        raise ValueError('missing : in auth')
    _, key = auth.split(':', 1)
    u = u._replace(netloc=netloc)
    return (u.geturl(), key)
