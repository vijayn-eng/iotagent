-- Deployed by Terraform (needs the uploaded artifact id).
CREATE FUNCTION `reroute_order`
AS 'io.confluent.proddemo.RerouteOrder'
USING JAR 'confluent-artifact://${artifact_id}';
