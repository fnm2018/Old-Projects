--移动端，web端的一二级渠道
set hive.mapred.supports.subdirectories=true;
set mapreduce.input.fileinputformat.input.dir.recursive=true;

INSERT OVERWRITE TABLE dm.tmp_loan_market_life_time_value partition (dt='${p_date}')   --渠道激活表 （直接聚合为该渠道的激活总数）
select a.time_created,
       a.cateagory,
       a.channel,
       count(if(a.channel<>'DAIKUAN_JIEDIANQIAN',a.active_num,null)) as active_num,
       case when a.sdk_type in ('I','R','S','W','L','J','B','X','A','U','C','M','P','G','T') then '1'
            when a.sdk_type in ('N','F')  then '2'
       end as sdk_type
  from
      (SELECT from_unixtime(cast(time_created/1000 AS bigint), 'yyyy-MM-dd')AS time_created,
              CASE  WHEN UPPER(channel) LIKE "%APP_STORE%" THEN "IOS"
                    when channel='yifan' then "IOS"
                    WHEN (UPPER(channel) LIKE "DAIKUAN_BAIDUSEM%") OR (UPPER(channel) LIKE "DAIKUAN_SOUGOUSEM%")
                             OR (channel LIKE "DAIKUAN_SHENMA%") THEN "SEM_ANDROID"
                    ELSE "ANDROID"
              END AS cateagory,
              channel,
              ad_id AS active_num,
              sdk_type
         FROM ods_sdm.sdm_advertisement_relation
        WHERE from_unixtime(cast(time_created/1000 AS bigint), 'yyyyMMdd') <= '${p_date}'
          and sdk_type IN ('N','F','I','R','S','W','L','J','B','X','A','U','C','M','P','G','T')
          AND channel IS NOT NULL --I是现金借款，N是洋钱罐借款，E无敌鸭
          AND time_first_access IS NOT NULL
       UNION ALL    --上面是移动端，下面是h5
       SELECT t1.time_updated,
              CASE WHEN (t2.stat LIKE "baidu%" OR t2.stat LIKE "sougou%") THEN "SEM_H5"
                   when t2.ads_type="I" then "信息流"
                   ELSE "H5"
              END AS category,
              t2.stat as channel,
              t1.mobile AS active_num,
              t1.sdk_type
         FROM (select from_unixtime(cast(time_updated/1000 AS bigint), 'yyyy-MM-dd') as time_updated,
                      mobile,
                      sdk_type,
                      act_id,
                      row_number() OVER (PARTITION BY mobile  ORDER BY bigint(time_updated) desc ) as rn
                 from ods_fdm.fdm_web_register_mobile_relation
                WHERE dt = '${p_date}'
                  and from_unixtime(cast(time_updated/1000 AS bigint), 'yyyyMMdd') <= '${p_date}'
                  and sdk_type IN ('N','F','I','R','S','W','L','J','B','X','A','U','C','M','P','G','T')
               )t1
          LEFT JOIN ods_fdm.fdm_web_operation_activity_config t2
            ON t1.act_id = t2.id
           and t1.rn=1
           and t2.dt = '${p_date}'
         where t1.rn=1
    ) a
  group by a.time_created,
           a.cateagory,
           a.channel,
           case when a.sdk_type in ('I','R','S','W','L','J','B','X','A','U','C','M','P','G','T') then '1'
                when a.sdk_type in ('N','F')  then '2'
           end
  ;

INSERT OVERWRITE TABLE dm.dm_loan_market_life_time_value_new
select '${p_date}' p_date,--job执行日期
        nvl(a.account_create_date,b.time_created) as account_create_date,
        nvl(a.first_channel,b.first_channel) as first_channel,
        nvl(a.second_channel,b.channel_name) as channel_name,
        nvl(a.business_type,b.business_type) as business_type,
        '0' as device_type,
        '0' as uv_num,
        a.active_num, --激活数
        a.registered_num,  --注册数
        a.done_num,--完件数
        a.done_num_1,--当天完件数
        a.done_num_3,
        a.done_num_7,
        a.done_num_15,
        a.done_num_30,
        a.pass_num,--过件数
        a.loan_num,--借款数
        '0' as risk_num,
        a.all_service_fee, ---之前的all_interest_cost字段名,不考虑坏账的服务费
        a.cost,--市场投入花费
        '0' as risk_principal,
        '${p_date}' time_update,
        sum(a.pass_num) over (partition by a.account_create_date) as all_pass_num,--这天总的过件数
        sum(a.done_num) over (partition by a.account_create_date) as all_done_num,  --这天总完件数
        a.actual_pass_num,--实际过件数
        a.sum_service_fee,  --考虑坏账率的服务费
        a.sum_guarantee_fee,  --考虑坏账率的质保金
        a.sum_interest,--考虑坏账率的利息
        a.risk_cost, --风险成本
        a.sum_principal,--放款金额
        a.pass_num_1,--当天过件
        a.pass_num_3,
        a.pass_num_7,
        a.pass_num_30,
        a.actual_pass_num_1,--当天实际过件
        a.actual_pass_num_3,
        a.actual_pass_num_7,
        a.actual_pass_num_30,
        a.loan_num_1,--当天借款
        a.loan_num_3,
        a.loan_num_7,
        a.loan_num_30,
        a.loan_principal_1,--当天放款金额
        a.loan_principal_3,
        a.loan_principal_7,
        a.loan_principal_30,
        '0'as y_sum_guarantee_fee,
        '0'as other_sum_guarantee_fee,
        nvl(a.funding_income,0) as funding_income, --机构资金收入 as y_sum_service_fee
        nvl(a.funding_cost_principal,0) as funding_cost_principal, --资金方成本（--as other_sum_service_fee,
        b.bill7_principal,--7天应还本金
        b.registered_num as registing_num,  --最新分区registered_num，每个分区registing_num
        b.sum_m1_7  --逾期7天本金
  from (
  SELECT t.p_date AS account_create_date, --注册日期
         nvl(tc.cateagory,'UNKNOW')AS first_channel, --一级渠道
         nvl(t.channel_name,'UNKNOW') as second_channel, --二级渠道
         CASE
             WHEN t.app_id='1' THEN "现金借款"
             WHEN t.app_id='2' THEN "洋钱罐借款"
         END AS business_type,
         '0' UV_num,
         max(cast(nvl(t0.active_num,0) as int)) active_num, --已经按渠道的激活数，取最大就可以
         count(t.loan_account_id) registered_num, --注册人数
         count(t1.loan_account_id)done_num,--完件人数
         sum(CASE
                WHEN t1.LOAN_ACCOUNT_ID IS NOT NULL AND t.p_date = t1.TIME_CREATED THEN
                1 ELSE 0 END) done_num_1, --当天累计完件人数
         sum(CASE
                WHEN t1.LOAN_ACCOUNT_ID IS NOT NULL AND
                t1.TIME_CREATED between t.p_date and date_add(t.p_date, 2) THEN
                1 ELSE 0 END) done_num_3, --3天内累计完件人数
         sum(CASE
                WHEN t1.LOAN_ACCOUNT_ID IS NOT NULL AND
                t1.TIME_CREATED between t.p_date and date_add(t.p_date, 6) THEN
                1 ELSE 0 END) done_num_7, --7天内累计完件人数
         sum(CASE
                WHEN t1.LOAN_ACCOUNT_ID IS NOT NULL AND
                t1.TIME_CREATED between t.p_date and date_add(t.p_date, 14) THEN
                1 ELSE 0 END) done_num_15 ,--15天内累计完件人数
         sum(CASE
                WHEN t1.LOAN_ACCOUNT_ID IS NOT NULL AND
                t1.TIME_CREATED between t.p_date and date_add(t.p_date, 29) THEN
                1 ELSE 0 END) done_num_30 ,--30天内累计完件人数
         sum(if (t1.credits_status='A',1,0)) pass_num,--过件人数
         sum(if (t1.credits_status='A' and bigint(t1.total_credits) not in('500','1000','2000','1500'),1,0)) actual_pass_num,  --实际过件人数实际过件人数（去掉首次过件额度为1500和3000的用户）
         count(t2.account_id) as loan_num, --借款人数
         sum(nvl(t2.principal,0)) as sum_principal,--成功借款本金
         sum(if(t2.funding_provider = 'Y' ,(1-coalesce(b.risk,b1.risk,0))*t2.guarantee_fee,0)) as sum_guarantee_fee,--洋钱罐质保金
         sum(if(t2.funding_provider = 'Y',t2.service_fee,0))  as all_service_fee  ,   --不考虑坏账率的服务费
         sum(if(t2.funding_provider = 'Y' ,(1-coalesce(b.risk,b1.risk,0))*t2.service_fee,0))  AS sum_service_fee, --洋钱罐服务费
         sum((1-coalesce(b.risk,b1.risk,0))*t2.interest)  AS sum_interest, --利息（洋钱罐和机构资金，未拆分）
         sum(coalesce(b.risk,b1.risk,0) * t2.principal) as risk_cost,--风险成本
         sum(if(t2.funding_provider <> 'Y', (1-coalesce(b.mob2,b1.mob2,0))*t2.index_1_fee +
                                            (1-coalesce(b.mob3,b1.mob3,0))*t2.index_2_fee +
                                            (1-coalesce(b.risk,b1.risk,0))*(t2.all_fee - t2.index_1_fee - t2.index_2_fee),0 )
            )as funding_income, --机构资金收入
         sum(case when t2.funding_provider = 'Y'  and t2.origin_terms='12' then nvl(t2.principal,0)* 0.0777
                  when t2.funding_provider = 'Y'  and t2.origin_terms='6' then nvl(t2.principal,0)* 0.0413
                  when t2.funding_provider = 'Y'  and t2.origin_terms='3' then nvl(t2.principal,0)* 0.0235
                  when t2.funding_provider <>'Y'  and t2.origin_terms='12' then nvl(t2.principal,0)* 0.0718
                  when t2.funding_provider <> 'Y' and t2.origin_terms='6' then nvl(t2.principal,0)* 0.0359
                  when t2.funding_provider <> 'Y' and t2.origin_terms='3' then nvl(t2.principal,0)* 0.01795
                  else 0
             end )as funding_cost_principal,--增加资金成本
         max(cast(nvl(cost,0) as double)) cost, --获客成本
         sum(if (t1.credits_status='A' and t.p_date = t1.TIME_ACCEPTED ,1,0)) pass_num_1, --当天累计过件人数
         sum(if (t1.credits_status='A' and t1.TIME_ACCEPTED between t.p_date and date_add(t.p_date, 2) ,1,0)) pass_num_3,--3天累计过件人数
         sum(if (t1.credits_status='A' and t1.TIME_ACCEPTED between t.p_date and date_add(t.p_date, 6) ,1,0)) pass_num_7,--7天累计过件人数
         sum(if (t1.credits_status='A' and t1.TIME_ACCEPTED between t.p_date and date_add(t.p_date, 29) ,1,0)) pass_num_30,--30天累计过件人数
         sum(if (t1.credits_status='A' and bigint(t1.total_credits) not in('500','1000','2000','1500') and t.p_date = t1.TIME_ACCEPTED ,1,0)) actual_pass_num_1, --当天实际累计过件人数（去掉首次过件额度为1500和3000的用户）
         sum(if (t1.credits_status='A' and bigint(t1.total_credits) not in('500','1000','2000','1500') and t1.TIME_CREATEDCEPTED between t.p_date and date_add(t.p_date, 2),1,0)) actual_pass_num_3, --3天实际累计过件人数（去掉首次过件额度为1500和3000的用户）
         sum(if (t1.credits_status='A' and bigint(t1.total_credits) not in('500','1000','2000','1500') and t1.TIME_ACCEPTED between t.p_date and date_add(t.p_date, 6),1,0)) actual_pass_num_7, --7天实际累计过件人数（去掉首次过件额度为1500和3000的用户）
         sum(if (t1.credits_status='A' and bigint(t1.total_credits) not in('500','1000','2000','1500') and t1.TIME_ACCEPTED between t.p_date and date_add(t.p_date, 29),1,0)) actual_pass_num_30, --30天实际累计过件人数（去掉首次过件额度为1500和3000的用户）
         sum(if(t2.account_id is not null and t2.time_payout=t.p_date,1,0)) loan_num_1,--当天借款人数
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 2),1,0)) loan_num_3,--3天累计借款人数
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 6),1,0)) loan_num_7,--7天累计借款人数
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 29),1,0)) loan_num_30,--30天累计借款人数
         sum(if(t2.account_id is not null and t2.time_payout=t.p_date,t2.principal,0)) loan_principal_1,--当天成功借款金额
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 2),t2.principal,0)) loan_principal_3,--3天累计成功借款金额
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 6),t2.principal,0)) loan_principal_7,--7天累计成功借款金额
         sum(if(t2.account_id is not null and t2.time_payout between t.p_date and date_add(t.p_date, 29),t2.principal,0)) loan_principal_30--30天累计成功借款金额
    FROM (SELECT from_unixtime(cast(a.TIME_CREATED / 1000 AS bigint), 'yyyy-MM-dd') p_date,
                 a.loan_account_id,
                 case when a.act_id is null then channel_name
                      else b.stat   --act_id 为null是移动端，非null是H5，第三方比较特殊是act_id 为null时的channel_name='DAIKUAN_JIEDIANQIAN'
                 end as channel_name,
                 c.app_id
            FROM ods_fdm.fdm_loan_register_channel_info a
            LEFT JOIN (SELECT id,
                              stat
                         FROM ods_fdm.fdm_web_operation_activity_config
                         WHERE dt = '${p_date}'
                        )b
              ON a.act_id = b.id
            LEFT JOIN (SELECT id,
                              app_id,  --app_id=1 是'现金借款'，2是洋钱罐借款 4是无敌鸭，目前与sdk_type的I，N，E基本一对一
                              from_unixtime(cast(TIME_CREATED / 1000 AS bigint),'yyyy-MM-dd') p_date
                         FROM ods_fdm.fdm_loan_account
                        WHERE dt = '${p_date}'
                          and app_id in ('1','2')
                        )c
                on c.id = a.loan_account_id
            WHERE a.dt = '${p_date}'
              and from_unixtime(cast(a.TIME_CREATED / 1000 AS bigint), 'yyyyMMdd') <= '${p_date}'
         )t
    LEFT JOIN (select if(channel='DAIKUAN_JIEDIANQIAN','THIRD_PARTY', cateagory) as cateagory, --最细粒度的渠道名对应的一级渠道
                      channel
                 from dm.tmp_loan_market_life_time_value
                where dt='${p_date}'
                  and channel <> 'DAIKUAN_CASH_LOAN'
                group by cateagory,channel
                union all
                select 'ANDROID'as cateagory ,
                      'DAIKUAN_CASH_LOAN' as channel
              )tc
      on tc.channel=t.channel_name  --匹配对应的一级渠道
    LEFT JOIN (select time_created,
                      cateagory,
                      channel,
                      active_num,
                      sdk_type
                 from dm.tmp_loan_market_life_time_value
                where dt='${p_date}'
              )t0  --计算激活数
      ON t0.time_created=t.p_date
     AND t0.channel=t.channel_name
     and t0.sdk_type=t.app_id
    LEFT JOIN (SELECT a.LOAN_ACCOUNT_ID,
                      a.trace_id,
                      a.credits_status,
                      b.total_credits,
                      from_unixtime(cast(a.TIME_CREATED / 1000 AS bigint), 'yyyy-MM-dd') TIME_CREATED,
                      from_unixtime(cast(a.TIME_ACCEPTED / 1000 AS bigint), 'yyyy-MM-dd') TIME_ACCEPTED
                 FROM ods_fdm.fdm_loan_user_credits_info a
                 left join dw.dw_loan_user_first_credits_info b
                    on a.LOAN_ACCOUNT_ID=b.LOAN_ACCOUNT_ID
                WHERE a.dt = '${p_date}'
                  and from_unixtime(cast(a.TIME_CREATED / 1000 AS bigint), 'yyyyMMdd') <= '${p_date}'
              )t1  --用户分值
      ON t1.LOAN_ACCOUNT_ID = t.loan_account_id
    LEFT JOIN (SELECT a.id,
                      a.account_id,
                      from_unixtime(cast(a.time_payout/ 1000 AS bigint), 'yyyy-MM-dd') time_payout,
                      a.service_fee,
                      a.origin_terms ,
                      a.guarantee_fee,
                      a.interest,
                      a.principal,
                      a.funding_provider,
                      a.product_user_type,
                      CAST(a.interest AS DOUBLE)+ CAST(a.service_fee AS DOUBLE)+
                      CAST(a.broker_fee AS DOUBLE)+ CAST(a.insurance_fee AS DOUBLE)+
                      CAST(a.guarantee_fee AS DOUBLE)+ CAST(a.guarantee_service_fee AS DOUBLE)+
                      CAST(a.security_fee AS DOUBLE)+ CAST(a.debt_management_fee AS DOUBLE)+
                      CAST(a.funding_service_fee AS DOUBLE) AS all_fee,--总应收费用
                      b.index_1_fee, --1期所有应收费用
                      c.index_2_fee --2期所有应收费用
                 FROM ods_fdm.fdm_cash_loan_order a
                 LEFT JOIN(SELECT order_id,
                                  sum( CAST(interest AS DOUBLE)+ CAST(service_fee AS DOUBLE)+
                                        CAST(broker_fee AS DOUBLE)+ CAST(insurance_fee AS DOUBLE)+
                                        CAST(guarantee_fee AS DOUBLE)+ CAST(guarantee_service_fee AS DOUBLE)+
                                        CAST(security_fee AS DOUBLE)+ CAST(debt_management_fee AS DOUBLE)+
                                        CAST(funding_service_fee AS DOUBLE)
                                      ) AS index_1_fee
                             FROM ods_fdm.fdm_cash_loan_instalment
                            WHERE dt = '${p_date}'
                              AND status IN ('I','C')
                              AND `index` = '1'
                            GROUP BY order_id
                          )b
                    ON a.id = b.order_id
                 LEFT JOIN(SELECT order_id,
                                  sum( CAST(interest AS DOUBLE)+ CAST(service_fee AS DOUBLE)+
                                        CAST(broker_fee AS DOUBLE)+ CAST(insurance_fee AS DOUBLE)+
                                        CAST(guarantee_fee AS DOUBLE)+ CAST(guarantee_service_fee AS DOUBLE)+
                                        CAST(security_fee AS DOUBLE)+ CAST(debt_management_fee AS DOUBLE)+
                                        CAST(funding_service_fee AS DOUBLE)
                                      ) AS index_2_fee
                             FROM ods_fdm.fdm_cash_loan_instalment
                            WHERE dt = '${p_date}'
                              AND status IN ('I','C')
                              AND `index` = '2'
                            group by order_id
                          )c
                    ON a.id = c.order_id
                WHERE a.dt ='${p_date}'
                  AND a.order_seq='1'
                  AND from_unixtime(cast(a.time_created/ 1000 AS bigint), 'yyyyMMdd')<= '${p_date}'
             )t2
      ON t2.account_id=t.loan_account_id  --订单统计
    left join (select a.order_id,
                      nvl(a.score_set,'473') as score_set,
                      a.total_credits,
                      a.origin_terms,
                      nvl(a.score,b.score) as score
                 from dw.dw_cashloan_order_risk_score a
                 left join (SELECT max(cast(score AS DOUBLE)) AS score,
                                   trace_id,
                                   rule_set_id
                            FROM ods_sdm.sdm_rule_set_hit_event_inc
                           WHERE score IS NOT NULL
                             and rule_set_id='473'
                           GROUP BY trace_id,
                                    rule_set_id
                           )b
                   on a.trace_id=b.trace_id
                  and nvl(a.score_set,'473')='473'
                where order_seq='1'
              )t3
      on t2.id=t3.order_id
    LEFT JOIN dm.dm_score_rate b   ---风险预估表
           ON nvl(b.rule_set_id,'473')=if(datediff('2018-12-12',t1.TIME_CREATED)>0 ,'433',if(t1.TIME_CREATED between '2018-12-12' and '2019-05-22', '441', nvl(t3.score_set,'473')) ) --没有匹配到给定的dm_risk_flow_id按rule_set_id=473算（大赦天下没有预估，默认都是按473）
          and b.dt='20200618'
          AND cast(b.credits as int)=cast(t3.total_credits as int)
          AND t3.origin_terms=b.terms
          and cast(b.score_bin_min as double) < cast(t3.score AS DOUBLE)
          AND cast(t3.score AS DOUBLE) <= cast(b.score_bin_max as double)
          and b.channel = IF(nvl(tc.cateagory,'UNKNOW') = "信息流","信息流","渠道量") --新增维度，用来区分信息流和渠道量
    LEFT JOIN dm.dm_score_rate b1   ---风险预估表
           ON cast(b1.credits as int)=cast(t3.total_credits as int)
          AND t3.origin_terms=b1.terms
          and cast(b1.score_bin_min as double) < cast(t3.score AS DOUBLE)
          AND cast(t3.score AS DOUBLE) <= cast(b1.score_bin_max as double)
          and b1.channel = IF(nvl(tc.cateagory,'UNKNOW') = "信息流","信息流","渠道量") --新增维度，用来区分信息流和渠道量
          and b1.dt='20200618'
          and nvl(b1.rule_set_id,'473')='473'
    LEFT JOIN (SELECT from_unixtime(cast(b.`date`/1000 AS bigint), 'yyyy-MM-dd')AS p_date,
                      case when b.channel_id='DAIKUAN_JIEDIANQIAN_ANDROID' then 'DAIKUAN_JIEDIANQIAN'
                            when b.channel_id = c.id then c.stat
                            else b.channel_id
                      end as channel_id,
                      sum(get_json_object(b.info,'$.cost')) as cost,--二级渠道花费总和（会出现一级渠道不同二级渠道相同的情况）
                      b.app_id
                 FROM ods_fdm.fdm_loan_channel_stat_daily b
                 LEFT JOIN (SELECT id,
                                    stat
                              FROM ods_fdm.fdm_web_operation_activity_config
                              WHERE dt = '${p_date}'
                            )c
                    ON b.channel_id = c.id
                where b.dt='${p_date}'
                  and from_unixtime(cast(b.`date`/1000 AS bigint), 'yyyyMMdd') <= '${p_date}'
                  and channel_id not in ('DAIKUAN_JIEDIANQIAN_NONE','DAIKUAN_JIEDIANQIAN_IOS','DAIKUAN_JIEDIANQIAN')
                group by from_unixtime(cast(b.`date`/1000 AS bigint), 'yyyy-MM-dd'),
                         case when b.channel_id='DAIKUAN_JIEDIANQIAN_ANDROID' then 'DAIKUAN_JIEDIANQIAN'
                              when b.channel_id = c.id then c.stat
                              else b.channel_id
                         end,
                         app_id
                )t4  --市场按时间，业务类型，渠道名称录入的花费
      ON t4.p_date=t.p_date
     AND t.channel_name=t4.channel_id
     and t4.app_id=t.app_id
    GROUP BY t.p_date , --注册日期
            nvl(tc.cateagory,'UNKNOW') ,
            nvl(t.channel_name,'UNKNOW'),
            CASE
                WHEN t.app_id='1' THEN "现金借款"
                WHEN t.app_id='2' THEN "洋钱罐借款"
            END
    )a
 full join dw.dw_market_roi_overdue b  --当日快照的注册数，应还本金和逾期本金
    on a.account_create_date=b.time_created
  and a.first_channel=b.first_channel
  and a.business_type=b.business_type
  and a.second_channel=b.channel_name
;
