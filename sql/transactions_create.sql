CREATE TABLE IF NOT EXISTS transactions (
  hash text NOT NULL PRIMARY KEY,
  transaction JSONB NOT NULL,
  block SERIAL REFERENCES blocks,
  created TIMESTAMP NOT NULL DEFAULT now()
)