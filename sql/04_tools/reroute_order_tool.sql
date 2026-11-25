CREATE TOOL `reroute_order_tool`
USING FUNCTION `reroute_order`
WITH (
  'type' = 'function',
  'description' = 'Move a work order from this line to another line''s MES dispatch list. Arguments: order_id, target_line, reason.'
);
