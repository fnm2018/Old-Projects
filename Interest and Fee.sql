create table risk_temp.fp_fee_crosscheck_part1_ghj

as

SELECT 
id as product_id, regexp_replace(idx,',','') as idx, t.provider_id as provider_id,
get_json_object(concat(j.str_decode,'}'),'$.principalPercent') as principalPercent, 
get_json_object(concat(j.str_decode,'}'),'$.serviceFeeRate') as serviceFeeRate, 
get_json_object(concat(j.str_decode,'}'),'$.guaranteeFeeRate') as guaranteeFeeRate,
get_json_object(concat(j.str_decode,'}'),'$.securityFeeRate') as securityFeeRate,
get_json_object(concat(j.str_decode,'}'),'$.interestFeeRate') as interestFeeRate,
get_json_object(concat(j.str_decode,'}'),'$.insuranceFeeRate') as insuranceFeeRate,
get_json_object(concat(j.str_decode,'}'),'$.debtManagementFeeRate') as debtManagementFeeRate

from ods_fdm.fdm_loan_product_config t

LATERAL VIEW
explode(split(substr(t.detailed_fee_rate_conf,2,length(t.detailed_fee_rate_conf)-2),'},')) j as str_decode
LATERAL VIEW
explode(array(substr(j.str_decode,10,2))) k as idx

where dt = '20200923' and enabled = 'T'

GROUP BY id, idx, t.provider_id,
get_json_object(concat(j.str_decode,'}'),'$.principalPercent'), 
get_json_object(concat(j.str_decode,'}'),'$.serviceFeeRate'), 
get_json_object(concat(j.str_decode,'}'),'$.guaranteeFeeRate'),
get_json_object(concat(j.str_decode,'}'),'$.securityFeeRate'),
get_json_object(concat(j.str_decode,'}'),'$.interestFeeRate'),
get_json_object(concat(j.str_decode,'}'),'$.insuranceFeeRate'),
get_json_object(concat(j.str_decode,'}'),'$.debtManagementFeeRate')

order by id

;
drop table risk_temp.fp_fee_crosscheck_part2_ghj
;
create table risk_temp.fp_fee_crosscheck_part2_ghj as
select * from 
(select fp.provider_code as provider_code, p1.provider_id as provider_id, p1.idx, p1.product_id as product_id,
(double(serviceFeeRate)+double(guaranteeFeeRate)+double(securityFeeRate)+double(insuranceFeeRate)+double(debtManagementFeeRate)) as total_fee,
rank() OVER (PARTITION BY p1.product_id ORDER BY CAST(idx AS BIGINT) DESC) AS rk
from risk_temp.fp_fee_crosscheck_part1_ghj p1

join ods_fdm.fdm_cash_loan_funding_provider_config fp

on fp.id = p1.provider_id and fp.dt = '20200924' 

group by fp.provider_code, p1.provider_id, p1.idx, p1.product_id,
(double(serviceFeeRate)+double(guaranteeFeeRate)+double(securityFeeRate)+double(insuranceFeeRate)+double(debtManagementFeeRate))) a
where a.rk = 1
;
SELECT * from risk_temp.fp_fee_crosscheck_part2_ghj 
order by product_id desc
limit 100
;

select * from 
risk_temp.fp_fee_instal_ghj_do_not_use 


;

select * 

from ods_fdm.fdm_loan_product_config pcon

where pcon.dt = '20200924'

order by id desc

limit 10
;


select distinct fp.provider_code 

from ods_fdm.fdm_loan_product_config pcon

join ods_fdm.fdm_cash_loan_funding_provider_config fp

on pcon.provider_id = fp.id and fp.dt = '20200924'

where pcon.dt = '20200924' --and pcon.enabled = 'T'

--order by provider_id desc

;
select
fp.terms,
if(fp.fee_rate_1 = round(p2.total_fee,3), 'TRUE', 'FALSE'),
if(fp.fee_rate_3 = round(p2.total_fee,3), 'TRUE', 'FALSE'),
if(fp.fee_rate_6 = round(p2.total_fee,3), 'TRUE', 'FALSE'),
if(fp.fee_rate_12 = round(p2.total_fee,3), 'TRUE', 'FALSE')

from risk_temp.fp_fee_crosscheck_part2_ghj p2

join risk_temp.fp_fee_instal_ghj_do_not_use fp 
on p2.product_id = fp.product_id 

;

SELECT * FROM risk_temp.fp_fee_instal_ghj_do_not_use
;

create table risk_temp.fp_fee_crosscheck_part3_ghj as
select * from 
(select fp.provider_code as provider_code, p1.provider_id as provider_id, p1.idx, p1.product_id as product_id,
(double(serviceFeeRate)+double(guaranteeFeeRate)+double(securityFeeRate)+double(insuranceFeeRate)+double(debtManagementFeeRate)) as total_fee,
rank() OVER (PARTITION BY p1.product_id ORDER BY CAST(idx AS BIGINT) asc) AS rk
from risk_temp.fp_fee_crosscheck_part1_ghj p1

join ods_fdm.fdm_cash_loan_funding_provider_config fp

on fp.id = p1.provider_id and fp.dt = '20200924' 

group by fp.provider_code, p1.provider_id, p1.idx, p1.product_id,
(double(serviceFeeRate)+double(guaranteeFeeRate)+double(securityFeeRate)+double(insuranceFeeRate)+double(debtManagementFeeRate))) a
where a.rk = 1
;

select
fp.terms,
if(fp.fee_rate_1 = round(p3.total_fee,3), 'TRUE', 'FALSE')

from risk_temp.fp_fee_crosscheck_part3_ghj p3

join risk_temp.fp_fee_instal_ghj_do_not_use fp 
on p3.product_id = fp.product_id 