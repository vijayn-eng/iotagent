CREATE TOOL `notify_supervisor_tool`
USING FUNCTION `notify_supervisor`
WITH (
  'type' = 'function',
  'description' = 'Send an andon / Teams notification to the line supervisor with the new plan. Arguments: line_id, message.'
);
