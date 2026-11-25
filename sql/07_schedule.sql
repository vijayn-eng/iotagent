-- Run the agent on every line snapshot and release the parsed schedule.
INSERT INTO `production_schedule`
WITH `planned` AS (
  SELECT
    `line_id`,
    CAST(`response` AS STRING) AS `raw_response`,
    REGEXP_EXTRACT(CAST(`response` AS STRING), '\{[\s\S]*\}', 0) AS `json_text`
  FROM `line_snapshots`,
  LATERAL TABLE(AI_RUN_AGENT(`production_scheduling_agent`, `snapshot_text`, `line_id`))
)
SELECT
  `line_id`,
  COALESCE(JSON_VALUE(`json_text`, '$.disruption'), 'UNKNOWN') AS `disruption`,
  COALESCE(CAST(JSON_VALUE(`json_text`, '$.severity') AS INT), 0) AS `severity`,
  COALESCE(JSON_QUERY(`json_text`, '$.sequence'), '[]') AS `sequence`,
  COALESCE(JSON_QUERY(`json_text`, '$.rerouted_orders'), '[]') AS `rerouted_orders`,
  COALESCE(JSON_QUERY(`json_text`, '$.orders_at_risk'), '[]') AS `orders_at_risk`,
  COALESCE(CAST(JSON_VALUE(`json_text`, '$.changeover_min') AS INT), 0) AS `changeover_min`,
  COALESCE(CAST(JSON_VALUE(`json_text`, '$.makespan_min') AS INT), 0) AS `makespan_min`,
  COALESCE(JSON_VALUE(`json_text`, '$.reasoning'), '') AS `reasoning`,
  COALESCE(JSON_QUERY(`json_text`, '$.actions_taken'), '[]') AS `actions_taken`,
  `raw_response`
FROM `planned`;
