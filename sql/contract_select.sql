SELECT json_build_object(
    'name', c.name,
    'tx_hash', c.tx_hash,
    'lst001', c.lst001,
    'lst002', c.lst002,
    'lst003', c.lst003,
    'code', c.code
)
FROM contracts c
WHERE name = %(c)s