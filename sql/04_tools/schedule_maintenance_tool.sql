CREATE TOOL `schedule_maintenance_tool`
USING FUNCTION `schedule_maintenance`
WITH (
  'type' = 'function',
  'description' = 'Open a CMMS maintenance work order and dispatch a technician to a machine. Arguments: machine_id, window (e.g. immediate, or next changeover at minute 95), reason.'
);
