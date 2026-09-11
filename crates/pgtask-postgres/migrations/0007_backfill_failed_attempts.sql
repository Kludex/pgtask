UPDATE pgtask.tasks AS tasks
SET failed_attempts = history.failed_attempts
FROM (
    SELECT task_id, count(*)::integer AS failed_attempts
    FROM pgtask.attempts
    WHERE state IN ('failed', 'lost')
    GROUP BY task_id
) AS history
WHERE tasks.id = history.task_id AND tasks.failed_attempts IS NULL;

UPDATE pgtask.tasks SET failed_attempts = 0 WHERE failed_attempts IS NULL;
