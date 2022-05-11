CREATE TABLE IF NOT EXISTS state_change (
  tx_hash text NOT NULL PRIMARY KEY REFERENCES transactions (hash),
  state jsonb NOT NULL,
  created TIMESTAMP NOT NULL DEFAULT now()
)