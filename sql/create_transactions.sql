CREATE TABLE IF NOT EXISTS transactions (
  hash text PRIMARY KEY,
  transaction JSONB NOT NULL,
  block SERIAL REFERENCES blocks,
  created TIMESTAMP DEFAULT now()
)