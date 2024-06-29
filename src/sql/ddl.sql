-- STAGING

-- drop table STV2024060715__STAGING.users;

create table STV2024060715__STAGING.users
(
    id              int PRIMARY KEY,
    chat_name       varchar(200),
    registration_dt timestamp,
    country         varchar(200),
    age             int CHECK (age >= 0)
)
order by id
SEGMENTED BY hash(id) all nodes;


-- drop table STV2024060715__STAGING.groups;

create table STV2024060715__STAGING.groups
(
    id              int PRIMARY KEY,
    admin_id        int,
    group_name      varchar(100),
    registration_dt timestamp,
    is_private      boolean,
    CONSTRAINT fk_groups_users_admin_id_from FOREIGN KEY (admin_id) references STV2024060715__STAGING.users (id)
)
order by id
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);


-- drop table STV2024060715__STAGING.dialogs;

create table STV2024060715__STAGING.dialogs
(
    message_id    int PRIMARY KEY,
    message_dt    timestamp,
    message_from  int,
    message_to    int,
    message       varchar(1000),
    message_group int
) 
order by message_id
SEGMENTED BY hash(message_id) ALL NODES
PARTITION BY message_dt::date
GROUP BY calendar_hierarchy_day(message_dt::date, 3, 2);


-- drop table STV2024060715__STAGING.group_log;

create table STV2024060715__STAGING.group_log
(
    group_id        int,
    user_id         int,
    user_id_from    int,
    event           varchar(10),
    event_datetime  timestamp,
    CONSTRAINT fk_group_log_groups_group_id FOREIGN KEY (group_id) references STV2024060715__STAGING.groups (id),
    CONSTRAINT fk_group_log_users_user_id_from FOREIGN KEY (user_id) references STV2024060715__STAGING.users (id),
    CONSTRAINT fk_group_log_users_user_id FOREIGN KEY (user_id_from) references STV2024060715__STAGING.users (id)
)
order by event_datetime
SEGMENTED BY hash(group_id) all nodes
PARTITION BY event_datetime::date
GROUP BY calendar_hierarchy_day(event_datetime::date, 3, 2);


-- DWH

-- drop table STV2024060715__DWH.h_users;

create table STV2024060715__DWH.h_users
(
    hk_user_id      bigint primary key,
    user_id         int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
; 

-- drop table STV2024060715__DWH.h_groups;

CREATE TABLE STV2024060715__DWH.h_groups
(
    hk_group_id        bigint primary key,
    group_id           int,
    registration_dt    datetime,
    load_dt            datetime,
    load_src           varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- drop table STV2024060715__DWH.h_dialogs;

CREATE TABLE STV2024060715__DWH.h_dialogs
(
    hk_message_id       bigint primary key,
    message_id          int,
    message_dt          datetime,
    load_dt             datetime,
    load_src            varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- drop table STV2024060715__DWH.l_admins;

create table STV2024060715__DWH.l_admins
(
    hk_l_admin_id   bigint primary key,
    hk_user_id      bigint not null,
    hk_group_id     bigint not null,
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_l_admin_user FOREIGN KEY (hk_user_id) REFERENCES STV2024060715__DWH.h_users (hk_user_id),
    CONSTRAINT fk_l_admin_group FOREIGN KEY (hk_group_id) REFERENCES STV2024060715__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.l_user_message;

create table STV2024060715__DWH.l_user_message
(
    hk_l_user_message   bigint primary key,
    hk_user_id          bigint not null,
    hk_message_id       bigint not null,
    load_dt             datetime not null,
    load_src            varchar(20),
    CONSTRAINT fk_l_user_message_user FOREIGN KEY (hk_user_id) REFERENCES STV2024060715__DWH.h_users (hk_user_id),
    CONSTRAINT fk_l_user_message_message FOREIGN KEY (hk_message_id) REFERENCES STV2024060715__DWH.h_dialogs (hk_message_id)
)
order by load_dt
SEGMENTED BY hk_l_user_message all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- drop table STV2024060715__DWH.l_groups_dialogs;

create table STV2024060715__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs     bigint primary key,
    hk_group_id             bigint not null,
    hk_message_id           bigint not null,
    load_dt                 datetime not null,
    load_src                varchar(20),
    CONSTRAINT fk_l_groups_dialogs_group FOREIGN KEY (hk_group_id) REFERENCES STV2024060715__DWH.h_groups (hk_group_id),
    CONSTRAINT fk_l_groups_dialogs_dialog FOREIGN KEY (hk_message_id) REFERENCES STV2024060715__DWH.h_dialogs (hk_message_id)
)
ORDER BY load_dt
SEGMENTED BY hk_l_groups_dialogs ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.l_user_group_activity;

create table STV2024060715__DWH.l_user_group_activity
(
    hk_l_user_group_activity    bigint primary key,
    hk_user_id                  bigint not null,
    hk_group_id                 bigint not null,
    load_dt                     datetime not null,
    load_src                    varchar(20),
    CONSTRAINT fk_l_user_group_activity_group FOREIGN KEY (hk_group_id) references STV2024060715__DWH.h_groups (hk_group_id),
    CONSTRAINT fk_l_user_group_activity_user FOREIGN KEY (hk_user_id) references STV2024060715__DWH.h_users (hk_user_id)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- drop table STV2024060715__DWH.s_user_socdem;

create table STV2024060715__DWH.s_user_socdem
(
    hk_user_id      bigint not null,
    country         varchar(200),
    age             int CHECK (age >= 0),
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_s_user_socdem_user FOREIGN KEY (hk_user_id) references STV2024060715__DWH.h_users (hk_user_id)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.s_user_chat_info;

create table STV2024060715__DWH.s_user_chat_info
(
    hk_user_id      bigint not null,
    chat_name       varchar(200),
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_s_user_chat_info_user FOREIGN KEY (hk_user_id) references STV2024060715__DWH.h_users (hk_user_id)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.s_group_name;

create table STV2024060715__DWH.s_group_name
(
    hk_group_id     bigint not null,
    group_name      varchar(100),
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_s_group_name_group FOREIGN KEY (hk_group_id) references STV2024060715__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.s_group_private_status;

create table STV2024060715__DWH.s_group_private_status
(
    hk_group_id     bigint not null,
    is_private      boolean,
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_s_group_private_status_group FOREIGN KEY (hk_group_id) references STV2024060715__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


-- drop table STV2024060715__DWH.s_admins;

create table STV2024060715__DWH.s_admins
(
    hk_l_admin_id   bigint not null,
    is_admin        boolean,
    admin_from      datetime,
    load_dt         datetime not null,
    load_src        varchar(20),
CONSTRAINT fk_s_admins_l_admins FOREIGN KEY (hk_l_admin_id) references STV2024060715__DWH.l_admins (hk_l_admin_id)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- drop table STV2024060715__DWH.s_dialog_info;

create table STV2024060715__DWH.s_dialog_info
(
    hk_message_id   bigint not null,
    message_from    int,
    message_to      int,
    message         varchar(1000),
    load_dt         datetime not null,
    load_src        varchar(20),
    CONSTRAINT fk_s_dialog_info_h_dialogs FOREIGN KEY (hk_message_id) references STV2024060715__DWH.h_dialogs (hk_message_id)
) 
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- drop table STV2024060715__DWH.s_auth_history;

create table STV2024060715__DWH.s_auth_history
(
    hk_l_user_group_activity    bigint not null,
    user_id_from                int,
    event                       varchar(10) ,
    event_dt                    datetime,
    load_dt                     datetime not null,
    load_src                    varchar(20),
    CONSTRAINT fk_s_auth_history_l_user_group_activity FOREIGN KEY (hk_l_user_group_activity) references STV2024060715__DWH.l_user_group_activity(hk_l_user_group_activity)
);
