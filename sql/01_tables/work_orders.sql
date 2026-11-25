-- MES dispatch list: the open work orders currently assigned to each line.
CREATE TABLE `work_orders` (
  `order_id` STRING NOT NULL,
  `line_id` STRING NOT NULL,
  `product_sku` STRING NOT NULL,
  `color` STRING NOT NULL,
  `quantity` INT NOT NULL,
  `run_minutes` INT NOT NULL,
  `priority` STRING NOT NULL,
  `due_in_min` INT NOT NULL,
  `material_id` STRING NOT NULL,
  `customer` STRING NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
);
