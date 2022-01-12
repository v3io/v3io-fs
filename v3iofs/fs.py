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
from os import environ
from urllib.parse import urlparse

from fsspec.spec import AbstractFileSystem
from v3io.dataplane import Client
import v3io

from .file import V3ioFile
from .path import split_container, unslash, path_equal, strip_schema
from .utils import handle_v3io_errors

_file_key = 'key'
_dir_key = 'prefix'


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
        self._client = _new_client(v3io_api, v3io_access_key)

    def ls(self, path, detail=True, marker=None, **kwargs):
        """Lists files & directories under path"""

        full_path = path
        path = strip_schema(path)
        container, path = split_container(path)
        if not container:
            return self._list_containers(detail)

        resp = self._client.get_container_contents(
            container=container,
            path=path,
            get_all_attributes=True,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
            marker=marker,
        )

        # Ignore 404's here
        if resp.status_code not in {200, 404}:
            raise Exception(
                f'{resp.status_code} received while accessing {path!r}')

        out = (
                _resp_dirs(resp, container, detail) +
                _resp_files(resp, container, detail)
        )

        if not marker:
            # first time in
            if not _has_data(resp):
                return [self._ls_file(container, path, detail)]
            if not out:
                raise FileNotFoundError(f'{full_path!r} not found')

        if hasattr(resp.output, 'next_marker') and resp.output.next_marker:
            marker = resp.output.next_marker
            out.extend(self.ls(full_path, detail, marker))
        return out

    def _ls_file(self, container, path, detail, marker=None):
        # '/a/b/c' -> ('/a/b', 'c')
        dirname, _, filename = path.rpartition('/')
        resp = self._client.get_container_contents(
            container=container,
            path=dirname,
            get_all_attributes=True,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
            marker=marker,
        )

        full_path = f'/{container}/{path}'
        handle_v3io_errors(resp, full_path)
        contents = getattr(resp.output, 'contents', [])
        objs = [obj for obj in contents if path_equal(obj.key, path)]
        if not objs:
            if hasattr(resp.output, 'next_marker') and resp.output.next_marker:
                marker = resp.output.next_marker
                return self._ls_file(container, path, detail, marker)
            else:
                raise FileNotFoundError(full_path)

        obj = objs[0]
        if not detail:
            return full_path
        return info_of(container, obj, _file_key)

    def _list_containers(self, detail):
        resp = self._client.get_containers(
            raise_for_status=v3io.dataplane.RaiseForStatus.never)
        handle_v3io_errors(resp, 'containers')
        fn = container_info if detail else container_path
        return [fn(c) for c in resp.output.containers]

    def copy(self, path1, path2, **kwargs):
        ...  # FIXME

    def _rm(self, path):
        container, path = split_container(path)
        if not container:
            raise ValueError(f'bad path: {path:r}')

        resp = self._client.delete_object(
            container=container,
            path=path,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
        )

        # Ignore 404's and 409's in delete
        if resp.status_code not in {200, 204, 404, 409}:
            raise Exception(
                f'{resp.status_code} received while accessing {path!r}')

    def touch(self, path, truncate=True, **kwargs):
        if not truncate:  # TODO
            raise ValueError('only truncate touch supported')

        path = strip_schema(path)
        container, path = split_container(path)
        resp = self._client.put_object(
            container, path,
            raise_for_status=v3io.dataplane.RaiseForStatus.never
        )

        handle_v3io_errors(resp, path)

    def info(self, path, **kw):
        """Details of entry at path

        Returns a single dictionary, with exactly the same information as
        ``ls`` would with ``detail=True``.

        Parameters
        ----------
        path: str
            Path to get info for
        **kw:
            Keyword arguments passed to `ls`

        Returns
        -------
        dict
            keys: name (full path in the FS), size (in bytes), type (file,
            directory, or something else) and other FS-specific keys.
        """

        path_with_container = strip_schema(path)
        container, path_without_container = split_container(path_with_container)

        # First, we try to get the file's attributes, which will fail with a 404 if it's actually a directory.
        resp = self._client.get_item(container,
                                     path_without_container,
                                     attribute_names=['__size', '__mtime_secs', '__mtime_nsecs', '__mode', '__gid', '__uid'],
                                     raise_for_status=v3io.dataplane.RaiseForStatus.never)

        if resp.status_code == 200:
            mtime = int(resp.output.item['__mtime_secs']) + int(resp.output.item['__mtime_nsecs']) / 10 ** 9
            entry = {
                'name': path_with_container,
                'type': 'file',
                'size': resp.output.item['__size'],
                'mtime': mtime,
                'mode': resp.output.item['__mode'],
                'gid': resp.output.item['__gid'],
                'uid': resp.output.item['__uid'],
            }
            return entry
        elif resp.status_code == 404:
            pass  # The file may still be a directory.
        else:
            raise Exception(f'{resp.status_code} received while getting the attributes of {path_with_container!r}')

        # Check the existence of a directory at the provided path.
        resp = self._client.get_container_contents(
            container=container,
            path=path_without_container,
            raise_for_status=v3io.dataplane.RaiseForStatus.never,
            limit=0
        )

        if resp.status_code == 200:
            return {'name': path_with_container, 'size': 0, 'type': 'directory'}
        elif resp.status_code == 404:
            raise FileNotFoundError(path_with_container)
        else:
            raise Exception(f'{resp.status_code} received while listing {path_with_container!r}')

    def _open(
            self,
            path,
            mode='rb',
            block_size=None,
            autocommit=True,
            cache_type='readahead',
            cache_options=None,
            **kw,
    ):
        return V3ioFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kw,
        )


def container_path(container):
    return f'/{container.name}'


def parse_time(creation_date):
    # '2020-03-26T09:42:57.504000+00:00'
    # '2020-03-26T09:42:57.71Z'
    i = creation_date.rfind('+')  # If not found will be -1, good for Z
    dt = datetime.strptime(creation_date[:i], '%Y-%m-%dT%H:%M:%S.%f')
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def container_info(container):
    return {
        'name': container.name,
        'size': None,
        'created': parse_time(container.creation_date),
    }


_missing = object()
_extra_obj_attrs = [
    # dest, src, convert
    ('created', 'creating_time', parse_time),
    ('atime', 'access_time', parse_time),
    ('mode', 'mode', lambda v: int(v[1:], base=8)),  # '040755'
    ('gid', 'gid', lambda v: int(v, 16)),
    ('uid', 'uid', lambda v: int(v, 16)),
]


def obj_path(container, obj, name_key):
    path = unslash(getattr(obj, name_key))
    return f'/{container}/{path}'


def info_of(container_name, obj, name_key):
    info = {
        'name': obj_path(container_name, obj, name_key),
        'type': 'file' if hasattr(obj, 'size') else 'directory',
        'size': getattr(obj, 'size', 0),
        'mtime': parse_time(obj.last_modified),
    }

    for src, dest, conv in _extra_obj_attrs:
        val = getattr(obj, src, _missing)
        if val is _missing:
            continue
        info[dest] = conv(val)

    return info


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


def _resp_dirs(resp, container, detail):
    if not hasattr(resp.output, 'common_prefixes'):
        return []

    objs = resp.output.common_prefixes
    if not detail:
        return [obj_path(container, obj, _dir_key) for obj in objs]

    return [info_of(container, obj, _dir_key) for obj in objs]


def _resp_files(resp, container, detail):
    if not hasattr(resp.output, 'contents'):
        return []

    objs = resp.output.contents
    if not detail:
        return [obj_path(container, obj, _file_key) for obj in objs]

    return [info_of(container, obj, _file_key) for obj in objs]


def _has_data(resp):
    out = resp.output
    return hasattr(out, 'common_prefixes') or hasattr(out, 'contents')


def _new_client(v3io_api=None, v3io_access_key=None) -> Client:
    v3io_api = v3io_api or environ.get('V3IO_API')
    v3io_access_key = \
        v3io_access_key or environ.get('V3IO_ACCESS_KEY')

    return Client(
        endpoint=v3io_api,
        access_key=v3io_access_key,
    )
