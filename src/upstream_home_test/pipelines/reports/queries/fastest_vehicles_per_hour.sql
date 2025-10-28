SELECT vin,
       date,
       hour,
       max_velocity_per_vin, row_num as rank
FROM (
    SELECT vin,
           date,
           hour,
           max_velocity_per_vin,
           ROW_NUMBER() OVER(PARTITION BY date,hour ORDER BY max_velocity_per_vin DESC) AS row_num
    FROM (
            SELECT vin,
                   EXTRACT(DAY FROM timestamp) as date,
                   EXTRACT(HOUR FROM timestamp) as hour,
                   max(velocity) as max_velocity_per_vin
            FROM aaa
            GROUP BY vin, EXTRACT(DAY FROM timestamp), EXTRACT(HOUR FROM timestamp)

        ) as a) as b
WHERE row_num < 11
ORDER BY date desc,hour desc
