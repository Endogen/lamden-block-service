# CLI with Typer to
# - Sync single block
# - Range of blocks
# - Download all blocks from GitHub
# - Sync old blocks (after download)
# - Check if all blocks in DB (with 'save_db=True' & 'save_file=True' params)
# - Check if all blocks on HDD (with 'save_db=True' & 'save_file=True' params)
# - Needed to create DB??
# - Put class BlockGrabber in own module so that it can be accessed by utils too

def is_valid_address(address: str) -> bool:
    """ Check if the given string is a valid Lamden address """
    if not len(address) == 64:
        return False
    try:
        int(address, 16)
    except:
        return False
    return True
