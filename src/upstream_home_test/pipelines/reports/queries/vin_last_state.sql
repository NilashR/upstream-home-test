WITH last_reported_vid AS
(SELECT vin, max(timestamp) as max_timestamp
 from aaa group by vin)
     ,
    last_reported_front_left_door_state as
    (
SELECT vin, frontLeftDoorState, timestamp,row_num
FROM (SELECT vin, frontLeftDoorState, timestamp, ROW_NUMBER() OVER(PARTITION BY vin ORDER BY timestamp DESC) AS row_num
     from aaa where frontLeftDoorState is not null)
WHERE row_num = 1),
    last_reported_wipers_state as
    (
SELECT vin, wipersState, timestamp,row_num
FROM (SELECT vin, wipersState, timestamp, ROW_NUMBER() OVER(PARTITION BY vin ORDER BY timestamp DESC) AS row_num
     from aaa where wipersState is not null)
WHERE row_num = 1)
select vidd.vin, vidd.max_timestamp,front_left_door_state.frontLeftDoorState,wipers_state.wipersState
from  last_reported_vid as vidd
left join last_reported_front_left_door_state as front_left_door_state
         on front_left_door_state.vin = vidd.vin
left join last_reported_wipers_state as wipers_state
         on wipers_state.vin = vidd.vin
--  where vidd.vin = '1C4NJDBB0GD610265'