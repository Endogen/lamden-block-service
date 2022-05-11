INSERT INTO current_state(tx_hash, key, state)
VALUES (%(txh)s, %(k)s, %(s)s)
ON CONFLICT (key) DO UPDATE SET tx_hash = %(txh)s, state = %(s)s