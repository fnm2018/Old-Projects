from datetime import date
import pandas as pd
import time
from bct.customized.swhy.mtc import MTC, to_dataclass
from bct.customized.swhy.report import OtcReport, OtcTrade, otc_trade_info_headers
from bct.customized.swhy.product import cash_flows
from bct.customized.swhy.pricer import price_cash_flows
from bct.customized.swhy.market import import_quotes_from_csv


valuation_date = date(2021, 3, 22)
# option_types = ['欧式看涨', '欧式看跌', '欧式二元看涨', '欧式二元看跌', '美式二元看涨', '看涨价差', '单向鲨鱼鳍看涨', '单向鲨鱼鳍看跌']
# option_types = ['看涨自动敲出赎回', '看跌自动敲出赎回', '雪球看涨自动敲出赎回', '收益凭证雪球',
#                 '逐步调整看涨自动敲出赎回', '逐步调整雪球看涨自动敲出赎回']
option_types = ['看涨价差']

s = time.time()
m = MTC(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\MyTotalContracts_2021-03-01.xlsx', valuation_date)
e = time.time()
print(f'loading mtc takes {e - s}s')

s = time.time()
report = OtcReport('C:\\Users\\admin\\Downloads\\20210301\\场外期权业务日报(2021-03-01).xlsx')
e = time.time()
print(f'loading reports takes {e - s}s')

s = time.time()
quotes = import_quotes_from_csv('./000905.csv')

e = time.time()
print(f'loading historical quotes takes {e - s}s')

contracts_df = report.contracts(option_types=option_types,
                                fields=['协议编号', '实际期权费', '行权费', '市值',
                                        'Delta', 'Gamma', 'Theta', 'Vega', 'Rho',
                                        'Delta金额', 'Gamma金额', 'Vanna', 'Vanna金额'])

pnl = []
otc_trade_report = []

s = time.time()
for _, row in m.df(option_types=option_types).iterrows():
    r = to_dataclass(row)
    cfs = cash_flows(r, valuation_date, quotes.get_quote)
    result = price_cash_flows(cfs, valuation_date, report.market_params)
    premium = sum(result.premium)
    pv = sum([o.price for o in result.option])
    delta = sum([o.delta for o in result.option])
    delta_cash = sum([o.delta_cash for o in result.option])
    gamma = sum([o.gamma for o in result.option])
    gamma_cash = sum([o.gamma_cash for o in result.option])
    vega = sum([o.vega for o in result.option])
    rho = sum([o.rho for o in result.option])
    theta = sum([o.theta for o in result.option])
    vanna = sum([o.vanna for o in result.option])
    vanna_cash = sum([o.vanna_cash for o in result.option])

    exercise_fee = sum(result.exercise_fee)
    contract_id = r.contract_id
    rec = {
        '协议编号': contract_id,
        '实际期权费': premium,
        '行权费': exercise_fee,
        '市值': pv,
        'Delta': delta,
        'Gamma': gamma,
        'Theta': theta,
        'Vega': vega,
        'Rho': rho,
        'Delta金额': delta_cash,
        'Gamma金额': gamma_cash,
        'Vanna': vanna,
        'Vanna金额': vanna_cash
    }
    pnl.append(rec)

    otc_trade_report.append(OtcTrade(
        id=r.id,
        contract_id=r.contract_id,
        counter_party=r.counter_party,
        contract_type=r.contract_type,
        option_type=r.option_type,
        code=r.code,
        not_amt=r.not_amt,
        maturity=r.maturity,
        begin_date=r.begin_date,
        end_date=r.end_date,
        deal_date=r.deal_date,
        initial_s=r.initial_s,
        h_strike_ratio=r.h_strike_ratio * r.initial_s,
        l_strike_ratio=r.l_strike_ratio * r.initial_s,
        h_touch_ratio=r.h_touch_ratio * r.initial_s,
        l_touch_ratio=r.l_touch_ratio * r.initial_s,
        floating_rate_1=r.rebate2,
        floating_rate_2=r.rebate3,
        floating_rate_3=r.rebate4,
        rebate=r.rebate,
        contract_premium_amount=r.actual_not_amt * r.total_premium,
        minimum_premium_amount=r.actual_not_amt * r.minimum_premium * r.p_ratio,
        front_premium=99999,
        actual_not_amt=r.actual_not_amt,
        actual_premium_amount=premium,
        exercise_fee=exercise_fee,
        max_payment=99999,
        intrinsic_value=99999,
        market_value=pv,
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        rho=rho,
        delta_cash=delta_cash,
        gamma_cash=gamma_cash,
        vanna=vanna,
        vanna_cash=vanna_cash,
        premium_payment_time='待定',
        contract_status=99999,
        investment_scale=99999,
        contract_state='待定',
        p_ratio=r.p_ratio,
        actual_end_date=r.actual_end_date,
        ob_ser_date=','.join([t.isoformat() for t in r.ob_ser_date]),
        pre_pay_amount=99999,
        pre_pay_fee_revenue=r.pre_pay_fee_revenue,
        pre_pay_fee_get=r.pre_pay_fee_get,
        new_investment_scale=99999,
        sales_fee=99999,
        direction='卖' if r.pos > 0 else '买',
        off_set='是' if r.off_set > 0 else '否',
        is_stock='待定',
        is_cross_border='是' if r.cross_border > 0 else '否'
    ))

e = time.time()
print(f'pricing takes {e - s}s')

pnl_df = pd.DataFrame(pnl)
diff_df = contracts_df.merge(pnl_df, how='outer', on=['协议编号'])

diff_df.to_csv('pnl_diff.csv', index=False, encoding='utf-8-sig')

otc_trade_report_df = pd.DataFrame(otc_trade_report)
otc_trade_report_df.columns = otc_trade_info_headers
otc_trade_report_df.to_csv('report.csv', index=False, encoding='utf-8-sig')
