insert into STV2024060715__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
select distinct
    hash(hg.hk_group_id, hu.hk_user_id) as hk_l_admin_id,
    hg.hk_group_id as hk_group_id,
    hu.hk_user_id as hk_user_id,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.groups as g
left join STV2024060715__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2024060715__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id, hu.hk_user_id) not in (select hk_l_admin_id from STV2024060715__DWH.l_admins);


insert into STV2024060715__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select distinct
    hash(hd.hk_message_id, hu.hk_user_id) as hk_l_user_message,
    hu.hk_user_id as hk_user_id,
    hd.hk_message_id as hk_message_id,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.dialogs as d
left join STV2024060715__DWH.h_users as hu on hu.user_id = d.message_from
left join STV2024060715__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hd.hk_message_id, hu.hk_user_id) not in (select hk_l_user_message from STV2024060715__DWH.l_user_message);


insert into STV2024060715__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select distinct
    hash(hg.hk_group_id, hd.hk_message_id) as hk_l_groups_dialogs,
    hd.hk_message_id as hk_message_id,
    hg.hk_group_id as hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.dialogs as d
left join STV2024060715__DWH.h_groups as hg on d.message_group = hg.group_id
left join STV2024060715__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hg.hk_group_id is not null 
    and hash(hg.hk_group_id, hd.hk_message_id) not in (select hk_l_groups_dialogs from STV2024060715__DWH.l_groups_dialogs);


insert into STV2024060715__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
    hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
    hu.hk_user_id as hk_user_id,
    hg.hk_group_id as hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.group_log as gl
left join STV2024060715__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2024060715__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV2024060715__DWH.l_user_group_activity);
