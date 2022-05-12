INSERT INTO blocks_invalid(block_num)
VALUES (%(bn)s)
ON CONFLICT (block_num) DO NOTHING