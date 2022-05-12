INSERT INTO blocks_missing(block_num)
VALUES (%(bn)s)
ON CONFLICT (block_num) DO NOTHING