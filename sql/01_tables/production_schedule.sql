-- Sink: the sequenced schedule the agent releases for each line.
CREATE TABLE `production_schedule` (
  `line_id` STRING NOT NULL,
  `disruption` STRING NOT NULL,
  `severity` INT NOT NULL,
  `sequence` STRING NOT NULL,
  `rerouted_orders` STRING NOT NULL,
  `orders_at_risk` STRING NOT NULL,
  `changeover_min` INT NOT NULL,
  `makespan_min` INT NOT NULL,
  `reasoning` STRING NOT NULL,
  `actions_taken` STRING NOT NULL,
  `raw_response` STRING NOT NULL
);
