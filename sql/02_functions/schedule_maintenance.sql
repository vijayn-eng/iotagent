-- Deployed by Terraform (needs the uploaded artifact id).
CREATE FUNCTION `schedule_maintenance`
AS 'io.confluent.proddemo.ScheduleMaintenance'
USING JAR 'confluent-artifact://${artifact_id}';
