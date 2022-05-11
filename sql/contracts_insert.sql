INSERT INTO contracts(tx_hash, name, code, lst001, lst002)
VALUES (%(txh)s, %(n)s, %(c)s, %(l1)s, %(l2)s)
ON CONFLICT (name) DO UPDATE SET tx_hash = %(txh)s, code = %(c)s, lst001 = %(l1)s, lst002 = %(l2)s