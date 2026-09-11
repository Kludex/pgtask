-- Storage protocol 2: step names keep their bounds and lose their character set.
--
-- A step name can appear in a free-form span attribute, but it does not name an identifier, a
-- NOTIFY channel, or a metric attribute. The charset only constrained libraries that compose step
-- names on a caller's behalf. Queue, task, schedule, and signal names keep the rule because those
-- can reach a CLI argument, a channel, or a bounded metric attribute.
--
-- The new predicate is implied by the old one, so every stored row already satisfies it and the
-- constraint is added NOT VALID to skip a scan of what is usually the largest table.

ALTER TABLE pgtask.checkpoints
    DROP CONSTRAINT checkpoints_step_name_check,
    ADD CONSTRAINT checkpoints_step_name_check
        CHECK (step_name <> '' AND octet_length(step_name) <= 255) NOT VALID;

ALTER TABLE pgtask.waits
    DROP CONSTRAINT waits_step_name_check,
    ADD CONSTRAINT waits_step_name_check
        CHECK (step_name <> '' AND octet_length(step_name) <= 255) NOT VALID;

ALTER TABLE pgtask.result_waits
    DROP CONSTRAINT result_waits_step_name_check,
    ADD CONSTRAINT result_waits_step_name_check
        CHECK (step_name <> '' AND octet_length(step_name) <= 255) NOT VALID;

CREATE OR REPLACE FUNCTION pgtask.storage_protocol_version()
RETURNS integer
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 2;
$$;

-- Protocol 1 workers keep running against this schema: it only accepts more than they can send.
CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range()
RETURNS TABLE(minimum integer, maximum integer)
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1, 2;
$$;
