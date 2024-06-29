with
user_group_messages as (
     select
          hk_group_id,
          count(distinct hk_user_id) as cnt_users_in_group_with_messages
     from STV2024060715__DWH.l_user_group_activity luga
     where hk_user_id in (select distinct hk_user_id from STV2024060715__DWH.l_user_message)
     group by hk_group_id
),
user_group_log as (
     select
          luga.hk_group_id,
          count(distinct luga.hk_user_id) as cnt_added_users
     from STV2024060715__DWH.l_user_group_activity luga
     where luga.hk_l_user_group_activity in (
               select hk_l_user_group_activity
               from STV2024060715__DWH.s_auth_history
               where event = 'add')
          and luga.hk_group_id in (
               select hk_group_id
               from STV2024060715__DWH.h_groups
               order by registration_dt
               limit 10)
     group by luga.hk_group_id)
select ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users * 100, 2) as group_conversion
from user_group_log ugl
         left join user_group_messages ugm
                   on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;
