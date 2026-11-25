-- OT / IIoT telemetry per machine (PRESS, PAINT, ASSY on each line).
CREATE TABLE `machine_telemetry` (
  `machine_id` STRING NOT NULL,
  `line_id` STRING NOT NULL,
  `station` STRING NOT NULL,
  `status` STRING NOT NULL,
  `oee` DOUBLE NOT NULL,
  `temperature_c` DOUBLE NOT NULL,
  `vibration_mm_s` DOUBLE NOT NULL,
  `fault_code` STRING NOT NULL,
  `est_repair_min` INT NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
);
