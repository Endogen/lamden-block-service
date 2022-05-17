INSERT INTO current_state(tx_hash, key, value)
VALUES (%(txh)s, %(k)s, %(v)s)
ON CONFLICT (key) DO UPDATE SET tx_hash = %(txh)s, value = %(v)s