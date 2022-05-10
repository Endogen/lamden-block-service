CREATE TABLE IF NOT EXISTS current_state (
  tx_hash text NOT NULL REFERENCES transactions (hash),
  key text NOT NULL PRIMARY KEY,
  state jsonb NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT now(),
  created TIMESTAMP NOT NULL DEFAULT now()
)