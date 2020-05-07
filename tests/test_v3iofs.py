import fsspec
from v3iofs import V3ioFS


def test_register():
    cls = fsspec.get_filesystem_class(V3ioFS.protocol)
    assert cls is V3ioFS, 'not registered'

    options = {
        'v3io_api': 'a.b.com',
        'v3io_access_key': 's3cr3t',
    }
    fs = fsspec.filesystem('v3io', **options)
    assert isinstance(fs, V3ioFS), f'bad object class - {fs.__class__}'
