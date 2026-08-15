ALTER TABLE pgtask.tasks
ADD CONSTRAINT tasks_payload_size_check CHECK (octet_length(payload::text) <= 1048576),
ADD CONSTRAINT tasks_headers_size_check CHECK (octet_length(headers::text) <= 65536),
ADD CONSTRAINT tasks_result_size_check CHECK (result IS NULL OR octet_length(result::text) <= 1048576),
ADD CONSTRAINT tasks_error_size_check CHECK (error IS NULL OR octet_length(error::text) <= 262144);

ALTER TABLE pgtask.attempts
ADD CONSTRAINT attempts_error_size_check CHECK (error IS NULL OR octet_length(error::text) <= 262144);
