select 
     user_id
    ,click_time
    ,last_time
    ,diff_time
    ,flag
    ,row_number() over(partition by user_id,rn order by click_time asc)
from 
(
    select 
            user_id
            ,click_time
            ,last_time
            ,diff_time
            ,flag
            ,case when num1 = 0 or num1 is null then 0 else 1 end as rn
    from 
    (
        select 
            user_id
            ,click_time
            ,last_time
            ,diff_time
            ,flag
            ,(rn-flag) as num1

        from
        (
            select 
                user_id
                ,click_time
                ,last_time
                ,diff_time
                ,flag
                ,lag(flag) over(partition by user_id order by click_time) as rn

            from 
            (
                select 
                    t1.user_id
                    ,t1.click_time
                    ,t1.last_time
                    ,t1.diff_time
                    ,case when t1.diff_time > 30*60 then 1 else 0 end as flag
                from
                (
                    select 
                        t.user_id
                        ,t.click_time
                        ,t.last_time
                        ,unix_timestamp(t.click_time) -unix_timestamp(t.last_time) as diff_time
                    from 
                    (
                        select 
                             user_id
                            ,click_time
                            ,lag(click_time) over(partition by user_id order by click_time) as last_time
                        from user_clicklog
                    ) t
                ) t1
            ) t2
        ) t3
    ) t4
) t5
