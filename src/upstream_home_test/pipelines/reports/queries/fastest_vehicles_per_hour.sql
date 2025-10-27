SELECT vin,date, hour, max_velocity_per_vin, row_num as rank
FROM (
    SELECT vin,date, hour, max_velocity_per_vin, ROW_NUMBER() OVER(PARTITION BY date,hour ORDER BY max_velocity_per_vin DESC) AS row_num
    from (
            SELECT vin, EXTRACT(DAY FROM timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour,max(velocity) as max_velocity_per_vin
            from aaa
            group by vin, EXTRACT(DAY FROM timestamp), EXTRACT(HOUR FROM timestamp)

        ) as a) as b
WHERE row_num < 11
order by date desc,hour desc
