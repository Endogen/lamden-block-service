INSERT INTO blocks(block_num, block)
VALUES (%(bn)s, %(b)s)
ON CONFLICT (block_num) DO UPDATE SET block = %(b)s