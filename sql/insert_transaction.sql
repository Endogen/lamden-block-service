INSERT INTO transactions(hash, transaction)
VALUES (%(h)s, %(t)s)
ON CONFLICT (hash) DO UPDATE SET transaction = %(t)s