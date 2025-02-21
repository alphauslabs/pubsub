**New Query to get message throughput in Spanner. Taking messages per second, total messages, duration, and average write rate.**

```sql
WITH message_counts AS (
  SELECT 
    TIMESTAMP_TRUNC(createdAt, SECOND) AS second,
    COUNT(*) AS message_count
  FROM Messages
  WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
  GROUP BY second
),

stats AS (
  SELECT 
    MIN(second) AS start_time, 
    MAX(second) AS end_time, 
    SUM(message_count) AS total_messages
  FROM message_counts
)

SELECT 
  mc.second, 
  mc.message_count, 
  s.total_messages, 
  TIMESTAMP_DIFF(s.end_time, s.start_time, SECOND) AS total_time_seconds,
  SAFE_DIVIDE(s.total_messages, TIMESTAMP_DIFF(s.end_time, s.start_time, SECOND)) AS avg_messages_per_second
FROM message_counts mc
CROSS JOIN stats s
ORDER BY mc.second DESC;

```
OUTPUT:
![image](https://github.com/user-attachments/assets/954efeb7-ed71-4da1-b1e7-12c50ddc71b3)




**TESTED NEW CODES LOCALLY**
Added test client to test codes locally.
![f129eef1-0240-48fe-ba2e-35f0c64075fc](https://github.com/user-attachments/assets/a4c0f851-60b8-4b9a-a931-7adb8c1ec99e)
![df981bdb-55df-4979-b93b-02591f18eb95](https://github.com/user-attachments/assets/14823b12-053c-4ffb-97ed-8f6233b57c96)
