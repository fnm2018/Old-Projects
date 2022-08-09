create table risk_temp.fp_fee_instal_ghj_final_wopi_wd_last_2TD
as
select funding_provider as funding_provider, min(from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS')) as enabled_prox,
max(from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS')) as disabled_prox,
CASE WHEN o.product_user_type IN ('I', 'X', 'M') THEN 'IRR36产品'
else 'APR36产品' end product,
--o.product_id as product_id,
terms as terms, i.`index` as idx, 
round(i.interest/double(o.principal),4) as interest_rate,
round(i.service_fee/double(o.principal),4) service_fee_rate,
round(i.broker_fee/double(o.principal),4) broker_fee_rate,
round(i.insurance_fee/double(o.principal),4) insurance_fee_rate,
round(i.guarantee_fee/double(o.principal),4) guarantee_fee_rate,
round(i.guarantee_service_fee/double(o.principal),6) as guarantee_service_fee_rate,
round(i.security_fee/double(o.principal),4) security_fee_rate,
round(i.debt_management_fee/double(o.principal),4) debt_management_fee_rate,
round(i.funding_service_fee/double(o.principal),4) funding_service_fee_rate
--round(i.fee/double(o.principal),3) as Fee_Rate_Total


from ods_fdm.fdm_cash_loan_order o

LEFT JOIN
(SELECT order_id, `index`, interest, service_fee, broker_fee, insurance_fee, guarantee_fee, 
guarantee_service_fee, security_fee, debt_management_fee, funding_service_fee,
sum( CAST(interest AS DOUBLE)+ CAST(service_fee AS DOUBLE)+
CAST(broker_fee AS DOUBLE)+ CAST(insurance_fee AS DOUBLE)+
CAST(guarantee_fee AS DOUBLE)+ CAST(guarantee_service_fee AS DOUBLE)+
CAST(security_fee AS DOUBLE)+ CAST(debt_management_fee AS DOUBLE)+
CAST(funding_service_fee AS DOUBLE)
) AS fee
FROM ods_fdm.fdm_cash_loan_instalment instal
WHERE dt = ${dt}
AND status IN ('I','C')
GROUP BY order_id, `index`, interest, service_fee, broker_fee, insurance_fee, guarantee_fee, 
guarantee_service_fee, security_fee, debt_management_fee, funding_service_fee
) i
ON o.id = i.order_id


where o.dt = ${dt} and o.status in ('R','C') and from_unixtime(bigint(o.time_payout/1000),'yyyyMMdd') >= '20180101'
and o.funding_provider not like '%Y%' and o.funding_provider not like '%R%'
group by funding_provider, terms, i.index, --o.product_id, 
round(i.interest/double(o.principal),4),
round(i.service_fee/double(o.principal),4),
round(i.broker_fee/double(o.principal),4),
round(i.insurance_fee/double(o.principal),4),
round(i.guarantee_fee/double(o.principal),4),
round(i.guarantee_service_fee/double(o.principal),6),
round(i.security_fee/double(o.principal),4),
round(i.debt_management_fee/double(o.principal),4),
round(i.funding_service_fee/double(o.principal),4),
--round(i.fee/double(o.principal),3)

CASE WHEN o.product_user_type IN ('I', 'X', 'M') THEN 'IRR36产品'
else 'APR36产品' end
;



create table risk_temp.fp_fee_instal_ghj_final_wopi_wd_1
as
select funding_provider as funding_provider, min(from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS')) as enabled_prox,
max(from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS')) as disabled_prox,
o.product_user_type as product,
--o.product_id as product_id,
terms as terms, i.`index` as idx, 
round(i.interest/double(o.principal),4) as interest_rate,
round(i.service_fee/double(o.principal),4) service_fee_rate,
round(i.broker_fee/double(o.principal),4) broker_fee_rate,
round(i.insurance_fee/double(o.principal),4) insurance_fee_rate,
round(i.guarantee_fee/double(o.principal),4) guarantee_fee_rate,
round(i.guarantee_service_fee/double(o.principal),4) guarantee_service_fee_rate,
round(i.security_fee/double(o.principal),4) security_fee_rate,
round(i.debt_management_fee/double(o.principal),4) debt_management_fee_rate,
round(i.funding_service_fee/double(o.principal),4) funding_service_fee_rate
--round(i.fee/double(o.principal),3) as Fee_Rate_Total


from ods_fdm.fdm_cash_loan_order o

LEFT JOIN
(SELECT order_id, `index`, interest, service_fee, broker_fee, insurance_fee, guarantee_fee, 
guarantee_service_fee, security_fee, debt_management_fee, funding_service_fee,
sum( CAST(interest AS DOUBLE)+ CAST(service_fee AS DOUBLE)+
CAST(broker_fee AS DOUBLE)+ CAST(insurance_fee AS DOUBLE)+
CAST(guarantee_fee AS DOUBLE)+ CAST(guarantee_service_fee AS DOUBLE)+
CAST(security_fee AS DOUBLE)+ CAST(debt_management_fee AS DOUBLE)+
CAST(funding_service_fee AS DOUBLE)
) AS fee
FROM ods_fdm.fdm_cash_loan_instalment instal
WHERE dt = ${dt}
AND status IN ('I','C')
GROUP BY order_id, `index`, interest, service_fee, broker_fee, insurance_fee, guarantee_fee, 
guarantee_service_fee, security_fee, debt_management_fee, funding_service_fee
) i
ON o.id = i.order_id


where o.dt = ${dt} and o.status in ('R','C') and from_unixtime(bigint(o.time_payout/1000),'yyyyMMdd') >= '20180101'
and o.funding_provider not like '%Y%' and o.funding_provider not like '%R%'
group by funding_provider, terms, i.index, --o.product_id, 
round(i.interest/double(o.principal),4),
round(i.service_fee/double(o.principal),4),
round(i.broker_fee/double(o.principal),4),
round(i.insurance_fee/double(o.principal),4),
round(i.guarantee_fee/double(o.principal),4),
round(i.guarantee_service_fee/double(o.principal),4),
round(i.security_fee/double(o.principal),4),
round(i.debt_management_fee/double(o.principal),4),
round(i.funding_service_fee/double(o.principal),4),
--round(i.fee/double(o.principal),3)

o.product_user_type

;

drop table risk_temp.fp_fee_instal_ghj_final_wopi_wd_last1;
create table risk_temp.fp_fee_instal_ghj_final_wopi_wd_last1
as
select *
from
(select
o.funding_provider as funding_provider,
o.id as Order_id, o.terms as terms, o.principal as principal, if(o.product_user_type in ('I','X','M'), 'IRR36', 'ELSE') as product, 

wd.disabled_prox as disabled,
i.`index` as idx,
i.principal as instal_principal,
i.interest as interest_rate,
i.service_fee service_fee_rate,
i.broker_fee broker_fee_rate,
i.insurance_fee insurance_fee_rate,
i.guarantee_fee guarantee_fee_rate,
i.guarantee_service_fee guarantee_service_fee_rate,
i.security_fee security_fee_rate,
i.debt_management_fee debt_management_fee_rate,
i.funding_service_fee funding_service_fee_rate,
rank() OVER (PARTITION BY wd.disabled_prox    
ORDER BY CAST(o.id  AS BIGINT) DESC) AS rk

from ods_fdm.fdm_cash_loan_order o   

join risk_temp.fp_fee_instal_ghj_final_wopi_wd_1 wd

on from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS') = wd.disabled_prox
and o.funding_provider = wd.funding_provider and o.product_user_type = wd.product

join ods_fdm.fdm_cash_loan_instalment i

on i.`index` = wd.idx and i.order_id = o.id and i.dt = ${dt}

where o.dt = ${dt} and o.product_user_type in ('I','X','M')) a

where a.rk = 1
;


drop table risk_temp.fp_fee_instal_ghj_final_wopi_wd_last2;
create table risk_temp.fp_fee_instal_ghj_final_wopi_wd_last2
as
select *
from
(select
o.funding_provider as funding_provider,
o.id as Order_id, o.terms as terms, o.principal as principal, if(o.product_user_type not in ('I','X','M'), 'APR36', 'ELSE') as product, 

wd.disabled_prox as disabled,
i.`index` as idx,
i.principal as instal_principal,
i.interest as interest_rate,
i.service_fee service_fee_rate,
i.broker_fee broker_fee_rate,
i.insurance_fee insurance_fee_rate,
i.guarantee_fee guarantee_fee_rate,
i.guarantee_service_fee guarantee_service_fee_rate,
i.security_fee security_fee_rate,
i.debt_management_fee debt_management_fee_rate,
i.funding_service_fee funding_service_fee_rate,
rank() OVER (PARTITION BY wd.disabled_prox    
ORDER BY CAST(o.id  AS BIGINT) DESC) AS rk

from ods_fdm.fdm_cash_loan_order o   

join risk_temp.fp_fee_instal_ghj_final_wopi_wd_1 wd

on from_unixtime(bigint(o.time_created/1000),'yyyyMMdd HH:mm:SS') = wd.disabled_prox
and o.funding_provider = wd.funding_provider and o.product_user_type = wd.product

join ods_fdm.fdm_cash_loan_instalment i

on i.`index` = wd.idx and i.order_id = o.id and i.dt = ${dt}

where o.dt = ${dt} and o.product_user_type not in ('I','X','M')) a

where a.rk = 1
;