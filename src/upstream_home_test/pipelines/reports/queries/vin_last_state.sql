WITH last_reported_vid AS
(SELECT vin, max(timestamp) as max_timestamp
 from report_table group by vin)
     ,
    last_reported_front_left_door_state as
    (
SELECT vin, front_left_door_state, timestamp,row_num
FROM (SELECT vin, front_left_door_state, timestamp, ROW_NUMBER() OVER(PARTITION BY vin ORDER BY timestamp DESC) AS row_num
     from report_table where front_left_door_state is not null)
WHERE row_num = 1),
    last_reported_wipers_state as
    (
SELECT vin, wipers_state, timestamp,row_num
FROM (SELECT vin, wipers_state, timestamp, ROW_NUMBER() OVER(PARTITION BY vin ORDER BY timestamp DESC) AS row_num
     from report_table where wipers_state is not null)
WHERE row_num = 1)
select vidd.vin as vin, vidd.max_timestamp, front_left_door.front_left_door_state, wipers.wipers_state
from  last_reported_vid as vidd
left join last_reported_front_left_door_state as front_left_door
         on front_left_door.vin = vidd.vin
left join last_reported_wipers_state as wipers
         on wipers.vin = vidd.vin;
