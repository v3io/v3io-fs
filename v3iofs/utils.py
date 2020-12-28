_ignore_statuses = {200, 204, 206, 409}


def handle_v3io_errors(response, file_path):
    if response.status_code in _ignore_statuses:
        return response.body
    if response.status_code == 404:
        raise FileNotFoundError(f'{file_path!r} not found')
    raise Exception(
        f'{response.status_code} received while accessing {file_path!r}')
