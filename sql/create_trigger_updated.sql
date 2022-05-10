CREATE OR REPLACE TRIGGER set_timestamp
BEFORE UPDATE ON state_change
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp()