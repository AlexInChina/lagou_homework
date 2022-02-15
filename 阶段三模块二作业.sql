-- 1、找出全部夺得3连贯的队伍
with tmp as (
select 
team
,(year-row_number() over(partition by team order by year asc)) as diff_year
from t1
)
select 
temp.team
from
(select 
diff_year as diff_year
,max(team) as team
,count(team) as team_cnt
from tmp
group by diff_year
having team_cnt >= 3
) temp
group by team;
-- 2、找出每个id在在一天之内所有的波峰与波谷值
with tmp2 as (
select 
id,
time,
price,
lead_price,
price-lead_price as diff_price,
lag(price-lead_price) over(partition by id order by time asc) as lag_diff
from(
select 
id,
time,
price,
lead(price) over(partition by id order by time asc) as lead_price
from 
(select 
id,
date_format(concat("2022-02-13 ",time,":00"),"yyyy-MM-dd HH:mm:ss") as time,
price
from t2) t
) t2
)
select
t3.id as id,
substr(t3.time,12,5) as time,
t3.price as price,
case when diff_price > 0 then '波峰' else '波谷' end as feature
from
(
select
id,
time,
price,
lead_price,
diff_price,
case when diff_price*lag_diff<0 then 1 else 0 end as feature
from tmp2
) t3
where t3.feature = 1
;

-- 3、写SQL
-- 3.1、每个id浏览时长、步长 
-- 方法一：
select 
id as id
,(max(unix_timestamp(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd"))-min(unix_timestamp(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd")))/60 as `时长`
,count(browseid) as `步长`
from t3
group by id

-- 方法二：(想不到两数相减函数) --方法可能行不通 没有相关分钟日期相减函数
select 
id
,dt 
,count(browseid) over(partition by id order by unix_timestamp(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd") asc)
,相差函数(browseid) over(partition by id order by unix_timestamp(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd") asc
	rows between unbounded preceding and unbounded following)
from t3

-- substring(dt,12,2)
-- ,substring(next_dt,15,2)
-- 3.2、如果两次浏览之间的间隔超过30分钟，认为是两个不同的浏览时间；再求每个id浏览时长、步长
with temp as (
select
id
,dt
,last_dt
,diff_dt
,case when diff_dt > 30 then 1 else 0 end as flag
from (
select
id
,dt
,last_dt
,((substring(dt,12,2)*60+substring(dt,15,2))-(substring(last_dt,12,2)*60+substring(last_dt,15,2))) as diff_dt
from
(
select 
id
,date_format(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd") as dt
,lag(dt,1) over(partition by id order by unix_timestamp(concat(replace(dt,"/","-"),":00"),"yyyy-MM-dd HH:mm:dd") asc) as last_dt
,browseid
from t3
) t
) tt
)

select
id
,gap
,count(1)
from
(
select 
id
,dt
,flag
,rn
,max_dt
,min_dt
,(substring(max_dt,12,2)*60+substring(max_dt,15,2)) - (substring(min_dt,12,2)*60+substring(min_dt,15,2)) as gap
from
(
select
id
,dt
,flag
,rn
,max(dt) over(partition by id order by rn rows between unbounded preceding and unbounded following) max_dt
,min(dt) over(partition by id order by rn rows between unbounded preceding and unbounded following) min_dt
from (
select 
id
,dt
,flag
,(rn - flag + 1) as rn
from (
select 
id
,dt
,flag
,row_number() over(partition by id,flag order by dt) rn
from temp
) temp0
) temp1
) temp2
) temp3
group by id,gap

