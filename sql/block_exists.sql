SELECT exists (
  SELECT 1
  FROM blocks
  WHERE block_num = %(bn)s LIMIT 1
)