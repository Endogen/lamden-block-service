INSERT INTO transactions(hash, transaction, block)
VALUES (%(h)s, %(t)s, %(b)s)
ON CONFLICT (hash) DO UPDATE SET transaction = %(t)s, block = %(b)s