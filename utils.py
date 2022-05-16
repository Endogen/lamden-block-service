# TODO: CLI with Typer to
# TODO: Sync single block
# TODO: Sync range of blocks
# TODO: Download all blocks from ...
# TODO: Check if all blocks are in DB
# TODO: Check if all blocks on HDD
# TODO: Needed to create DB??

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
