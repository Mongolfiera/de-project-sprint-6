create table STV2024060715__STAGING.group_log
(
    group_id   integer ,
    user_id   integer ,
    user_id_from integer ,
    event varchar(10) ,
    event_datetime timestamp
)
order by event_datetime
SEGMENTED BY group_id all nodes
PARTITION BY event_datetime::date
GROUP BY calendar_hierarchy_day(event_datetime::date, 3, 2);

