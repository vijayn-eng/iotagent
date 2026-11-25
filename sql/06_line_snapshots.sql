-- Fuse orders + telemetry + inventory into ONE text snapshot per line per
-- planning burst (3s event-time SESSION window keyed by line_id).
-- Run with: SET 'sql.tables.scan.idle-timeout' = '5 s';
CREATE TABLE `line_snapshots` AS
WITH `unified` AS (
  SELECT `line_id`, 'order' AS `kind`, `event_time`,
         CONCAT('- ', `order_id`, ' | sku ', `product_sku`, ' | color ', `color`,
                ' | qty ', CAST(`quantity` AS STRING), ' | run ', CAST(`run_minutes` AS STRING), ' min',
                ' | priority ', `priority`, ' | due in ', CAST(`due_in_min` AS STRING), ' min',
                ' | material ', `material_id`, ' | customer ', `customer`) AS `line`
  FROM `work_orders`
  UNION ALL
  SELECT `line_id`, 'machine' AS `kind`, `event_time`,
         CONCAT('- ', `machine_id`, ' (', `station`, ') status=', `status`,
                ' oee=', CAST(`oee` AS STRING), ' temp=', CAST(`temperature_c` AS STRING), 'C',
                ' vibration=', CAST(`vibration_mm_s` AS STRING), 'mm/s',
                CASE WHEN `fault_code` <> ''
                     THEN CONCAT(' fault="', `fault_code`, '" est_repair=', CAST(`est_repair_min` AS STRING), 'min')
                     ELSE '' END) AS `line`
  FROM `machine_telemetry`
  UNION ALL
  SELECT `line_id`, 'material' AS `kind`, `event_time`,
         CONCAT('- ', `material_id`, ' (', `description`, ') on_hand=', CAST(`on_hand_qty` AS STRING),
                ' required=', CAST(`required_qty` AS STRING),
                CASE WHEN `on_hand_qty` < `required_qty`
                     THEN CONCAT(' SHORT BY ', CAST(`required_qty` - `on_hand_qty` AS STRING))
                     ELSE ' OK' END,
                ' inbound_eta=', CAST(`inbound_eta_min` AS STRING), 'min') AS `line`
  FROM `material_inventory`
)
SELECT
  `line_id`,
  `window_start`,
  `window_end`,
  CAST(SUM(CASE WHEN `kind` = 'order' THEN 1 ELSE 0 END) AS INT) AS `open_orders`,
  CONCAT(
    'Line: ', `line_id`, '\n\n',
    'Machines:\n', COALESCE(LISTAGG(CASE WHEN `kind` = 'machine' THEN `line` END, '\n'), '  (no telemetry)'), '\n\n',
    'Materials:\n', COALESCE(LISTAGG(CASE WHEN `kind` = 'material' THEN `line` END, '\n'), '  (none)'), '\n\n',
    'Open work orders (unsequenced):\n', COALESCE(LISTAGG(CASE WHEN `kind` = 'order' THEN `line` END, '\n'), '  (none)')
  ) AS `snapshot_text`
FROM TABLE(
  SESSION(TABLE `unified` PARTITION BY `line_id`, DESCRIPTOR(`event_time`), INTERVAL '3' SECONDS)
)
GROUP BY `line_id`, `window_start`, `window_end`;
