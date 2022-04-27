CREATE TABLE IF NOT EXISTS transactions (
  hash text PRIMARY KEY,
  transaction JSONB NOT NULL
)