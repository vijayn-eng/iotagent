-- Deployed by Terraform (needs the uploaded artifact id).
CREATE FUNCTION `notify_supervisor`
AS 'io.confluent.proddemo.NotifySupervisor'
USING JAR 'confluent-artifact://${artifact_id}';
