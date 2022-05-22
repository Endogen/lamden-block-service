def create_kill_script(path: str):
    import os

    with open(path, 'w') as f:
        f.truncate(0)
        f.write(f'{"#!/bin/bash"}\n\n{"kill -9 {os.getpid()}"}')

    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def is_valid_address(address: str) -> bool:
    """ Check if the given string is a valid Lamden address """
    if not address:
        return False
    if not len(address) == 64:
        return False
    try:
        int(address, 16)
    except:
        return False
    return True


def unwrap_fixed(value: str) -> str:
    if type(value) is dict and len(value) == 1 and '__fixed__' in value:
        return value['__fixed__']
    else:
        return value
