-- Deployed by Terraform (needs the uploaded artifact id).
CREATE FUNCTION `expedite_material`
AS 'io.confluent.proddemo.ExpediteMaterial'
USING JAR 'confluent-artifact://${artifact_id}';
