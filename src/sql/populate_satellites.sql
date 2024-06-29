insert into STV2024060715__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select distinct
    hu.hk_user_id as hk_user_id,
    su.country as country,
    su.age as age,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.users as su
left join STV2024060715__DWH.h_users as hu on su.id = hu.user_id
where hu.hk_user_id not in (select distinct hk_user_id from STV2024060715__DWH.s_user_socdem);


insert into STV2024060715__DWH.s_user_chat_info(hk_user_id, chat_name, load_dt, load_src)
select distinct
    hu.hk_user_id as hk_user_id,
    su.chat_name as chat_name,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.users as su
left join STV2024060715__DWH.h_users as hu on su.id = hu.user_id
where hu.hk_user_id not in (select distinct hk_user_id from STV2024060715__DWH.s_user_chat_info);


insert into STV2024060715__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
select distinct
    hg.hk_group_id as hk_group_id,
    sg.group_name,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.groups as sg
left join STV2024060715__DWH.h_groups as hg on sg.id = hg.group_id
where hg.hk_group_id not in (select distinct hk_group_id from STV2024060715__DWH.s_group_name);


insert into STV2024060715__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select distinct
    hg.hk_group_id as hk_group_id,
    sg.is_private,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.groups as sg
left join STV2024060715__DWH.h_groups as hg on sg.id = hg.group_id
where hg.hk_group_id not in (select distinct hk_group_id from STV2024060715__DWH.s_group_private_status);


insert into STV2024060715__DWH.s_admins(hk_l_admin_id, is_admin, admin_from, load_dt, load_src)
select distinct
    la.hk_l_admin_id as hk_l_admin_id,
    True as is_admin,
    hg.registration_dt as admin_from,
    now() as load_dt,
    's3' as load_src
from STV2024060715__DWH.l_admins as la
left join STV2024060715__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id
where la.hk_l_admin_id not in (select distinct hk_l_admin_id from STV2024060715__DWH.s_admins);


insert into STV2024060715__DWH.s_dialog_info(hk_message_id, message_from, message_to, message, load_dt, load_src)
select distinct
    hd.hk_message_id as hk_message_id,
    d.message_from as message_from,
    d.message_to as message_to,
    d.message message,
    now() as load_dt,
    's3' as load_src
from STV2024060715__DWH.h_dialogs as hd
left join STV2024060715__STAGING.dialogs as d on hd.message_id = d.message_id;


insert into STV2024060715__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select distinct
    lug.hk_l_user_group_activity as hk_l_user_group_activity,
    gl.user_id_from as user_id_from,
    gl.event as event,
    gl.event_datetime as event_dt,
    now() as load_dt,
    's3' as load_src
from STV2024060715__STAGING.group_log as gl
left join STV2024060715__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2024060715__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2024060715__DWH.l_user_group_activity as lug on (hg.hk_group_id = lug.hk_group_id) and (hu.hk_user_id = lug.hk_user_id)
where lug.hk_l_user_group_activity not in (select distinct hk_l_user_group_activity from STV2024060715__DWH.s_auth_history)
    and gl.event = 'add' or gl.event = 'leave';
