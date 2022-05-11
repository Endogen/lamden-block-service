INSERT INTO addresses(address)
VALUES (%(a)s)
ON CONFLICT (address) DO NOTHING