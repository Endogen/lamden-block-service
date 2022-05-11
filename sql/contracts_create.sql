CREATE TABLE IF NOT EXISTS contracts (
  tx_hash text NOT NULL REFERENCES transactions (hash),
  name text NOT NULL PRIMARY KEY,
  code text NOT NULL,
  lst001 BOOLEAN NOT NULL,
  lst002 BOOLEAN NOT NULL,
  created TIMESTAMP NOT NULL DEFAULT now()
)