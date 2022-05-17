CREATE TABLE IF NOT EXISTS current_state (
  tx_hash text NOT NULL REFERENCES transactions (hash),
  key text NOT NULL PRIMARY KEY,
  value jsonb,
  updated TIMESTAMP NOT NULL DEFAULT now(),
  created TIMESTAMP NOT NULL DEFAULT now()
)