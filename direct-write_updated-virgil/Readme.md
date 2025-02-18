**SQL query that groups messages by the createdAt timestamp stored in Spanner to measure messages per second from Spanner's perspective.**

```sql
WITH MessageCounts AS (
    SELECT
        TIMESTAMP_TRUNC(createdAt, SECOND) AS second,
        COUNT(*) AS message_count
    FROM
        Messages
    WHERE
        createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
    GROUP BY
        second
),
Summary AS (
    SELECT
        COUNT(*) AS total_messages,
        TIMESTAMP_DIFF(MAX(createdAt), MIN(createdAt), SECOND) AS total_time_seconds
    FROM
        Messages
    WHERE
        createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
)
SELECT
    second,
    message_count,
    (SELECT total_messages FROM Summary) AS total_message_count,
    (SELECT total_time_seconds FROM Summary) AS total_time_seconds,
    CASE
        WHEN (SELECT total_time_seconds FROM Summary) > 0 THEN (SELECT total_messages FROM Summary) / (SELECT total_time_seconds FROM Summary)
        ELSE 0
    END AS avg_messages_per_second
FROM
    MessageCounts
ORDER BY
    second DESC;
```
OUTPUT:
![image](https://github.com/user-attachments/assets/dfac2a66-ae4c-4ffc-8023-0a885ced69aa)
