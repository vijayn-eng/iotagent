-- WMS line-side inventory vs. what the open orders on the line require.
CREATE TABLE `material_inventory` (
  `material_id` STRING NOT NULL,
  `line_id` STRING NOT NULL,
  `description` STRING NOT NULL,
  `on_hand_qty` INT NOT NULL,
  `required_qty` INT NOT NULL,
  `inbound_eta_min` INT NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
);
