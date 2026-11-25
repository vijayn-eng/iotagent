-- Claude on Amazon Bedrock, via the connection Terraform creates.
CREATE MODEL `scheduling_model`
INPUT (`prompt` STRING)
OUTPUT (`response` STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = 'bedrock-prodsched-connection',
  'bedrock.params.max_tokens' = '8192'
);
