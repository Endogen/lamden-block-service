INSERT INTO state_change(tx_hash, state)
VALUES (%(txh)s, %(s)s)
ON CONFLICT (tx_hash) DO UPDATE SET state = %(s)s