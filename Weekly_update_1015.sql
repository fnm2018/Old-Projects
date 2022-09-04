create table risk_temp.loan_restraining_ghj
  
as

select * from

(select 
id as order_id, funding_provider as funding_provider, Product_Type as Product_Type, account_id as account_id, order_seq as order_seq,  status as status,    
product as product, time_created as time_created,
time_payout as time_payout, created_time_cand as created_time_cand, terms as terms, total_credits as total_credits, 
principal as principal, pro_id as pro_id, pro_terms as pro_terms,
row_number() over (partition by id, time_created, pro_terms) rn

from 
    
    (SELECT od.id AS id, 
             od.funding_provider AS funding_provider,
             op.funding_provider AS Product_Type,
             od.account_id AS account_id,      
             bigint(od.order_seq) AS order_seq,        
             od.status,   
             od.product_user_type product,      
             from_unixtime(CAST(od.time_created/1000 AS BIGINT),'yyyy-MM-dd HH:mm:SS') time_created,       
             from_unixtime(CAST(od.time_payout/1000 AS BIGINT),'yyyy-MM-dd HH:mm:SS') time_payout,     
             from_unixtime(CAST(cand.time_created/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:SS') created_time_cand,       
             od.terms terms,       
             od.principal principal,       
             crd.total_credits total_credits,      
             rank() OVER (PARTITION BY od.id      
                          ORDER BY CAST(cand.time_created AS BIGINT) DESC) AS rk,      
             pcon.id pro_id,       
             pcon.terms pro_terms     
             --pcon.min_credits min_credits,     
             --pcon.max_credits max_credits      
      FROM ods_fdm.fdm_cash_loan_order od      
      LEFT JOIN dw.dw_cashloan_order_credits_info crd 
      ON od.id = crd.order_id
      LEFT JOIN ods_sdm.sdm_dw_cash_loan_candidate_info_inc cand ON od.account_id = cand.loan_account_id 
      and from_unixtime(CAST(cand.time_created/1000 AS BIGINT),'yyyy-MM-dd HH:mm:SS') >= '2020-01-01 00:00:00'       
      AND CAST(od.time_created AS BIGINT) >= CAST(cand.time_created AS BIGINT)     
      LEFT JOIN ods_fdm.fdm_loan_product_config pcon ON pcon.dt = date_format(date_sub(current_date(), 1), 'yyyyMMdd')     
      AND cand.candidate_id = pcon.id
      left join dm.dm_cashloan_order_product op on op.id = od.id
      WHERE from_unixtime(CAST(od.time_created/1000 AS BIGINT),'yyyy-MM-dd HH:mm:SS') >= '2020-01-01 00:00:00' 
      --and od.order_seq <> '1' 可选择添加首复贷，或增加字段
      and od.dt = date_format(date_sub(current_date(), 1), 'yyyyMMdd') and od.status in ('R','C')) a        
       
      WHERE a.rk = 1) b

where b.rn = 1       

       
;  


--drop table risk_temp.middle_sheet_ghj_ppt;
create table risk_temp.middle_sheet_date
as 
select 
o.id, o.terms, o.principal, from_unixtime(bigint(o.time_payout/1000), 'yyyyMMdd') as time_payout, n.total_credits,
if(n1.order_id is null, 'FALSE', 'TRUE') 1_valid,
if(n3.order_id is null, 'FALSE', 'TRUE') 3_valid,
if(n6.order_id is null, 'FALSE', 'TRUE') 6_valid,
if(n12.order_id is null, 'FALSE', 'TRUE') 12_valid

from ods_fdm.fdm_cash_loan_order o 

left join risk_temp.loan_restraining_ghj n
on o.id = n.order_id

left join risk_temp.loan_restraining_ghj n1
on o.id = n1.order_id and n1.pro_terms = '1' 

left join risk_temp.loan_restraining_ghj n3
on o.id = n3.order_id and n3.pro_terms = '3'

left join risk_temp.loan_restraining_ghj n6
on o.id = n6.order_id and n6.pro_terms = '6'

left join risk_temp.loan_restraining_ghj n12
on o.id = n12.order_id and n12.pro_terms = '12'



where o.dt = date_format(date_sub(current_date(), 1), 'yyyyMMdd') and o.status in ('R', 'C') 
and from_unixtime(bigint(o.time_payout/1000), 'yyyyMMdd') between '20200101' and date_format(date_sub(current_date(), 1), 'yyyyMMdd')
;

create table risk_temp.last_part_middle_sheet_ghj_date
as
select m.id as order_id,

case when m.terms = '1' then 'Term_1'
 when m.terms = '24' then 'Term_24'

 when m.terms = '3' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUEFALSEFALSE' then 'Absolute_locked'
 when m.terms = '3' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUETRUEFALSE' then 'No12'
 when m.terms = '3' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUEFALSETRUE' then 'No6'
 when m.terms = '3' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUETRUETRUE' then 'Unlocked'

 when m.terms = '6' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'FALSETRUEFALSE' then 'Absolute_locked'
 when m.terms = '6' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUETRUEFALSE' then 'No12'
 when m.terms = '6' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'FALSETRUETRUE' then 'No3'
 when m.terms = '6' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUETRUETRUE' then 'Unlocked'

 when m.terms = '12' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'FALSEFALSETRUE' then 'Absolute_locked'
 when m.terms = '12' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUEFALSETRUE' then 'No6'
 when m.terms = '12' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'FALSETRUETRUE' then 'No3'
 when m.terms = '12' and concat(m.`3_valid`,m.`6_valid`,m.`12_valid`) = 'TRUETRUETRUE' then 'Unlocked'

end as locked_or_not, 

case when m.3_valid = 'TRUE' and bigint(m.total_credits) >= 8000 then 'Voluntarily_lower_loan'
     when m.3_valid = 'FALSE' and bigint(m.total_credits) >= 8000 then 'Normal' 
end as Voluntarily_lower_loan_amount_or_not

from risk_temp.middle_sheet_ghj_date m
;

create table risk_temp.middle_sheet_ghj_date as
select * from
(select m.time_payout as time_payout, m.id as order_id, m.terms as terms, m.principal as principal, m.`1_valid` as 1_valid, m.`3_valid` as 3_valid, m.`6_valid` as 6_valid, m.`12_valid` as 12_valid,
m.total_credits as total_credits, l.locked_or_not as locked_or_not, l.voluntarily_lower_loan_amount_or_not as voluntarily_lower_loan_amount_or_not, 
row_number() over(partition by m.id) rn

from
risk_temp.middle_sheet_date m

join 

risk_temp.last_part_middle_sheet_ghj_date l

on m.id = l.order_id) a 

where a.rn = 1 ;

--最新

select 
sum(case when t.`index` = 1 and t.extension_times = 0 then t.principal else 0 end)  index1_principal,--第一期应还金额
sum(case when t.`index` = 2 and t.extension_times = 0 then t.principal else 0 end)  index2_principal,--第二期应还金额
sum(case when t.`index` = 3 and t.extension_times = 0 then t.principal else 0 end)  index3_principal,--第三期应还金额
sum(case when t.`index` = 4 and t.extension_times = 0 then t.principal else 0 end)  index4_principal,--第四期应还金额
sum(case when t.`index` = 5 and t.extension_times = 0 then t.principal else 0 end)  index5_principal,--第五期应还金额
sum(case when t.`index` = 6 and t.extension_times = 0 then t.principal else 0 end)  index6_principal,--第六期应还金额
-- 一期逾期
sum(IF(t.`index` = 1 and t.extension_times = 0 , t.m1_1,  0)) index1_principal_1,--一期1天逾期本金
sum(IF(t.`index` = 1 and t.extension_times = 0 , t.m1_3,  0)) index1_principal_3,--一期3天逾期本金
sum(IF(t.`index` = 1 and t.extension_times = 0 , t.m1_7,  0)) index1_principal_7,--一期7天逾期本金
sum(IF(t.`index` = 1 and t.extension_times = 0 , t.m1_16,  0)) index1_principal_16,--一期16天逾期本金
sum(IF(t.`index` = 1 and t.extension_times = 0 , t.m2_31,  0)) index1_principal_31,--一期31天逾期本金
sum(order_principal) as order_principal,
sum(order_unpaid_principal) as order_unpaid_principal,
sum(order_unpaid_principal)/sum(order_principal) as rate,
count(1) as order_number,
sum(if(order_unpaid_principal > 0, 1, 0)) as overdue_number,
index,
time_payout,
product_user_type,
terms,
if(t.result = 'T','T','F') AB_test,
risk_flow_id


from

(select ord.account_id as account_id,
ord.id as order_id, 
ord.product_user_type,
ord.terms as terms, 
from_unixtime(cast(ord.time_payout/1000 as bigint), 'yyyy-MM') time_payout,
float(ord.principal) as order_principal, 
float(dmi.principal) as principal, 
ord.order_seq as order_seq,
i.`index` as index, 
nvl(float(b.order_unpaid_principal), 0) as order_unpaid_principal,
from_unixtime(cast(i.billing_date/1000 as bigint), 'yyyy-MM-dd') as billing_date, 
o.risk_flow_id as risk_flow_id,
date_add(from_unixtime(cast(i.billing_date/1000 as bigint), 'yyyy-MM-dd'), 31) <= '2020-08-02' as isvalid,
gk.result result,
dmi.extension_times as extension_times,
dmi.m1_1 m1_1,
dmi.m1_3 m1_3,
dmi.m1_7 m1_7,
dmi.m1_16 m1_16,
dmi.m2_31_v2 m2_31,
dmi.m3_61 m3_61,
dmi.m4_91 m3_91,
dmi.m5_121 m5_121,
dmi.m6_151 m3_151


from

risk_temp.${dt}_reloan_ppt1_v4 o

left join ods_fdm.fdm_cash_loan_order ord

on o.account_id = ord.account_id and ord.dt = ${dt} and ord.status in ('R','C')

left join dm.dm_order_instalment_info dmi

on dmi.order_id = ord.id and dmi.instalment_status in ('I','C') and dmi.order_status in ('R', 'C')

join 

(select *, row_number() over (partition by gk.user_id, gk.dt order by gk.time_created asc) rn 
from ods_sdm.sdm_gatekeeper_result_inc gk
where gk.dt > '20200820' and gk.user_id is not null and gk.project_id = '58') gk

on gk.user_id = ord.user_id and gk.rn = 1 and gk.dt = from_unixtime(cast(ord.time_created/1000 as bigint), 'yyyyMMdd')
--and from_unixtime(cast(gk.time_created/1000 as bigint), 'yyyyMMdd') > '20200917'

left join ods_fdm.fdm_cash_loan_instalment i 

on i.dt= ${dt} and i.status in ('C', 'I') and ord.id = i.order_id

left join ods_sdm.sdm_cash_loan_overdue_event_inc b

on i.id = b.instalment_id and b.overdue_days = 31

where o.status in ('R', 'C') 
and from_unixtime(cast(ord.time_payout/1000 as bigint), 'yyyy-MM-dd') > '2020-08-20') t

group by index,time_payout,product_user_type,terms, if(t.result = 'T','T','F'), risk_flow_id
;
 