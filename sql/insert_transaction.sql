INSERT INTO transactions(hash, transaction)
VALUES (%(h)s, %(t)s)
ON CONFLICT (hash) DO UPDATE SET block = %(t)s