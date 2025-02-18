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
![476897005_942324144684003_4433576153511227976_n](https://github.com/user![476872421_646427291388688_1801037326851234251_n](https://github.com/user-attachments/assets/d88a1193-b054-45d7-a6c9-170cf5301fe5)
-attachments/assets/f81600bc-570b-420b-a3da-aea6fd91eac0)
