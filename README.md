# v3iofs

<!--
[![CI](https://github.com/v3io/v3io-fs/workflows/CI/badge.svg)](https://github.com/v3io/v3io-fs/actions?query=workflow%3ACI)
-->
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


An [fsspec][fsspec] driver for [v3io][v3io].


## Examples


### Python

```python
>>> from v3iofs import V3ioFS  # Will register 'v3io' protocol in ffspec
>>> fs = V3ioFS('api.app.yh48.iguazio-cd2.com', v3io_access_key='s3cr3t')
>>> fs.ls('/container/path')
```

### Pandas

```python
# Use V3IO_ACCESS_KEY & V3IO_API from environment
>> df = pd.read_csv('v3io://container/path/to/file.csv')
```

### Dask

```python
>>> from v3iofs import V3ioFS
>>> from dask import bag

# Use V3IO_ACCESS_KEY & V3IO_API from environment
>>> url = 'v3io://container/path'
>>> file = bag.read_text(url)
>>> data, _ = file.compute()

# Pass key in storage_options
>>> storage_options={
...     'v3io_api': 'webapi.app.iguazio.com',
...     'v3io_access_key': 's3cr3t',
... }
>>> file = bag.read_text(url, storage_options=storage_options)
>>> data, _ = file.compute()
```

## Development


### Testing

You need to set `V3IO_ACCESS_KEY` and `V3IO_API` environment variables.
Then run `make test` to run the tests.


### Environment

Deployment requirements are in `requirements.txt` and development requirements
are in `dev-requirements.txt`.

```
$ python -m venv venv
$ ./venv/bin/python -m pip install -r dev-requirements.txt
```


[fsspec]: https://filesystem-spec.readthedocs.io
[v3io]: https://www.iguazio.com/docs/tutorials/latest-release/getting-started/containers/
