from typing import Callable
from bct.customized.swhy.priceable.snowball import SwhyAutocall, SwhyAutocallStep, SwhySnowballKnockIn, \
    SwhySnowballStepKnockIn, SwhySnowballSypz
from dataclasses import dataclass
from datetime import date
from ...analytics.pycore.holidays import embedded_calendar
from ...analytics.priceable.cash import CashPayment
from ...analytics.priceable.european import VanillaEuropean, DigitalCash
from ...analytics.priceable.feature import Position, OptionType, Security, BarrierDirection
from .priceable.barrier import SwhyOneTouch, SwhyKnockOut, SwhyDoubleSharkFin, SwhyAirbag, SwhyKnockOutParticipatory
from .priceable.european import SwhyOptionSpread, SwhyParticipatory, SwhyDoubleDigitalCash, \
    SwhyVanillaAmerican, SwhySharkFinTerminal, SwhyOptionRiskReversalVariant, SwhyConvex, SwhyRangeAccrual
from .mtc import MTCRow


@dataclass(frozen=True)
class CategorizedCashFlows:
    option: list[Position]
    premium: list[Position]
    exercise_fee: list[Position]


def _is_hk(code: str) -> bool:
    return code.endswith('HK')


def _is_settled(mtc: MTCRow, as_of: date) -> bool:
    return mtc.actual_end_date <= as_of


def _premium_flows(mtc: MTCRow) -> list[Position]:
    premium_payment_date = mtc.deal_date if '互换' in mtc.contract_type else mtc.begin_date
    return [Position(1, CashPayment(amount=mtc.premium_amount, payment_date=premium_payment_date))]


def _settled_flows(mtc: MTCRow) -> CategorizedCashFlows:
    return CategorizedCashFlows(option=[], premium=_premium_flows(mtc),
                                exercise_fee=[Position(1,
                                                       CashPayment(amount=mtc.exercise_fee,
                                                                   payment_date=mtc.deal_date))])


def _vanilla_european_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.h_strike_ratio * mtc.initial_s
    call = VanillaEuropean(strike=strike, expiration_date=expiration_date, option_type=OptionType.CALL,
                           delivery_date=delivery_date, underlying=Security(mtc.code))
    option_position = Position(quantity=quantity * mtc.pos, instrument=call)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _vanilla_european_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    # option
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.l_strike_ratio * mtc.initial_s
    put = VanillaEuropean(strike=strike, expiration_date=expiration_date, option_type=OptionType.PUT,
                          delivery_date=delivery_date, underlying=Security(mtc.code))
    option_position = Position(quantity=quantity * mtc.pos, instrument=put)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _vanilla_american_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.h_strike_ratio * mtc.initial_s
    call = SwhyVanillaAmerican(strike=strike, expiration_date=expiration_date, option_type=OptionType.CALL,
                               delivery_date=delivery_date, underlying=Security(mtc.code))
    option_position = Position(quantity=quantity * mtc.pos, instrument=call)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _vanilla_american_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    # option
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.l_strike_ratio * mtc.initial_s
    put = SwhyVanillaAmerican(strike=strike, expiration_date=expiration_date, option_type=OptionType.PUT,
                              delivery_date=delivery_date, underlying=Security(mtc.code))
    option_position = Position(quantity=quantity * mtc.pos, instrument=put)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _digital_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt * mtc.p_ratio
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.h_strike_ratio * mtc.initial_s
    payout = mtc.rebate
    digi_call = DigitalCash(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                            option_type=OptionType.CALL, delivery_date=delivery_date, payout=payout)
    option_position = Position(quantity=quantity * mtc.pos, instrument=digi_call)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _digital_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt * mtc.p_ratio
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.l_strike_ratio * mtc.initial_s
    payout = mtc.rebate
    digi_put = DigitalCash(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                           option_type=OptionType.PUT, delivery_date=delivery_date, payout=payout)
    option_position = Position(quantity=quantity * mtc.pos, instrument=digi_put)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _american_digital_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt * mtc.p_ratio
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    barrier = mtc.h_touch_ratio * mtc.initial_s
    am_digi_call = SwhyOneTouch(underlying=Security(mtc.code), barrier=barrier, direction=BarrierDirection.UP,
                                rebate=mtc.rebate, expiration_date=expiration_date, delivery_date=delivery_date)
    option_position = Position(quantity=quantity * mtc.pos, instrument=am_digi_call)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _american_digital_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt * mtc.p_ratio
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    barrier = mtc.l_touch_ratio * mtc.initial_s
    am_digi_put = SwhyOneTouch(underlying=Security(mtc.code), barrier=barrier, direction=BarrierDirection.DOWN,
                               rebate=mtc.rebate, expiration_date=expiration_date, delivery_date=delivery_date)
    option_position = Position(quantity=quantity * mtc.pos, instrument=am_digi_put)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _bull_spread(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    bull_spread = SwhyOptionSpread(underlying=Security(mtc.code), strike_low=strike_low, strike_high=strike_high,
                                   expiration_date=expiration_date, option_type=OptionType.CALL,
                                   delivery_date=delivery_date)
    option_position = Position(quantity=quantity * mtc.pos, instrument=bull_spread)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _bear_spread(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    bull_spread = SwhyOptionSpread(underlying=Security(mtc.code), strike_low=strike_low, strike_high=strike_high,
                                   expiration_date=expiration_date, option_type=OptionType.PUT,
                                   delivery_date=delivery_date)
    option_position = Position(quantity=quantity * mtc.pos, instrument=bull_spread)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _ko_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.h_strike_ratio * mtc.initial_s
    barrier = mtc.h_touch_ratio * mtc.initial_s
    rebate = mtc.rebate * mtc.initial_s
    ko = SwhyKnockOut(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                      option_type=OptionType.CALL, barrier=barrier, direction=BarrierDirection.UP,
                      rebate=rebate, delivery_date=delivery_date)
    ko_position = Position(quantity=quantity * mtc.pos, instrument=ko)
    return CategorizedCashFlows(option=[ko_position], premium=_premium_flows(mtc), exercise_fee=[])


def _ko_call_participatory(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    barrier = mtc.h_touch_ratio * mtc.initial_s
    rebate = mtc.rebate * mtc.initial_s
    ko = SwhyKnockOutParticipatory(underlying=Security(mtc.code), strike_low=strike_low, strike_high=strike_high,
                                   barrier=barrier, rebate=rebate, expiration_date=expiration_date,
                                   delivery_date=delivery_date, p_ratio1=mtc.p_ratio, p_ratio2=mtc.p_ratio2)
    ko_position = Position(quantity=quantity * mtc.pos, instrument=ko)
    return CategorizedCashFlows(option=[ko_position], premium=_premium_flows(mtc), exercise_fee=[])


def _ko_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.l_strike_ratio * mtc.initial_s
    barrier = mtc.l_touch_ratio * mtc.initial_s
    rebate = mtc.rebate * mtc.initial_s
    ko = SwhyKnockOut(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                      option_type=OptionType.PUT, barrier=barrier, direction=BarrierDirection.DOWN,
                      rebate=rebate, delivery_date=delivery_date)
    ko_position = Position(quantity=quantity * mtc.pos, instrument=ko)
    return CategorizedCashFlows(option=[ko_position], premium=_premium_flows(mtc), exercise_fee=[])


def _double_shark_fin(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    lower_strike = mtc.l_strike_ratio * mtc.initial_s
    upper_strike = mtc.h_strike_ratio * mtc.initial_s
    lower_barrier = mtc.l_touch_ratio * mtc.initial_s
    upper_barrier = mtc.h_touch_ratio * mtc.initial_s
    lower_rebate = mtc.rebate * mtc.initial_s
    upper_rebate = mtc.rebate * mtc.initial_s
    dko = SwhyDoubleSharkFin(underlying=Security(mtc.code), upper_strike=upper_strike, lower_strike=lower_strike,
                             expiration_date=expiration_date, upper_barrier=upper_barrier, lower_barrier=lower_barrier,
                             upper_rebate=upper_rebate, lower_rebate=lower_rebate, delivery_date=delivery_date)
    dko_position = Position(quantity=quantity * mtc.pos, instrument=dko)
    return CategorizedCashFlows(option=[dko_position], premium=_premium_flows(mtc), exercise_fee=[])


# 参与式看涨
def _participatory_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    p_ratio1 = mtc.p_ratio1
    p_ratio2 = mtc.p_ratio2
    participatory = SwhyParticipatory(underlying=Security(mtc.code), expiration_date=expiration_date,
                                      option_type=OptionType.CALL, delivery_date=delivery_date, strike_low=strike_low,
                                      strike_high=strike_high, p_ratio1=p_ratio1, p_ratio2=p_ratio2)
    participatory_position = Position(quantity=quantity * mtc.pos, instrument=participatory)
    return CategorizedCashFlows(option=[participatory_position], premium=_premium_flows(mtc), exercise_fee=[])


def _double_digital_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    payout_low = mtc.rebate
    payout_high = mtc.rebate2
    double_digi_call = SwhyDoubleDigitalCash(underlying=Security(mtc.code), strike_low=strike_low,
                                             strike_high=strike_high,
                                             expiration_date=expiration_date, option_type=OptionType.CALL,
                                             delivery_date=delivery_date, payout_low=payout_low,
                                             payout_high=payout_high)
    option_position = Position(quantity=quantity * mtc.pos, instrument=double_digi_call)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _shark_fin_terminal_call(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.h_strike_ratio * mtc.initial_s
    barrier = mtc.h_touch_ratio * mtc.initial_s
    rebate = mtc.rebate * mtc.initial_s
    ko = SwhySharkFinTerminal(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                              barrier=barrier, rebate=rebate, delivery_date=delivery_date)
    ko_position = Position(quantity=quantity * mtc.pos, instrument=ko)
    return CategorizedCashFlows(option=[ko_position], premium=_premium_flows(mtc), exercise_fee=[])


def _shark_fin_terminal_put(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike = mtc.l_strike_ratio * mtc.initial_s
    barrier = mtc.l_touch_ratio * mtc.initial_s
    rebate = mtc.rebate * mtc.initial_s
    ko = SwhySharkFinTerminal(underlying=Security(mtc.code), strike=strike, expiration_date=expiration_date,
                              barrier=barrier, rebate=rebate, delivery_date=delivery_date)
    ko_position = Position(quantity=quantity * mtc.pos, instrument=ko)
    return CategorizedCashFlows(option=[ko_position], premium=_premium_flows(mtc), exercise_fee=[])


def _risk_reversal_variant(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike1 = mtc.l_strike_ratio * mtc.initial_s
    strike2 = mtc.h_strike_ratio * mtc.initial_s
    strike3 = mtc.l_touch_ratio * mtc.initial_s
    strike4 = mtc.h_touch_ratio * mtc.initial_s
    p_ratio1 = mtc.p_ratio
    p_ratio2 = mtc.p_ratio
    p_ratio3 = mtc.p_ratio
    p_ratio4 = mtc.p_ratio
    risk_reversal_variant = SwhyOptionRiskReversalVariant(strike1=strike1, strike2=strike2, strike3=strike3,
                                                          strike4=strike4, p_ratio1=p_ratio1, p_ratio2=p_ratio2,
                                                          p_ratio3=p_ratio3, p_ratio4=p_ratio4,
                                                          expiration_date=expiration_date, delivery_date=delivery_date,
                                                          underlying=Security(mtc.code))
    option_position = Position(quantity=quantity * mtc.pos, instrument=risk_reversal_variant)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _convex(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    strike_low = mtc.l_strike_ratio * mtc.initial_s
    strike_high = mtc.h_strike_ratio * mtc.initial_s
    payout = mtc.rebate
    convex = SwhyConvex(underlying=Security(mtc.code), strike_low=strike_low, strike_high=strike_high,
                        expiration_date=expiration_date, option_type=OptionType.PUT, delivery_date=delivery_date,
                        payout=payout)
    option_position = Position(quantity=quantity * mtc.pos, instrument=convex)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc),
                                exercise_fee=[])


def _snowball_konck_in(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    begin_date = mtc.begin_date
    observation_dates = mtc.ob_ser_date
    initial_spot = mtc.initial_s
    knockout_barrier = mtc.h_touch_ratio * initial_spot
    rebate_rate = mtc.rebate
    premium_rate = mtc.pre_pay_fee * mtc.pre_pay_fee_revenue + (mtc.premium / mtc.p_ratio) * mtc.pos
    knockin_barrier = mtc.l_touch_ratio * initial_spot
    knockin_strike = mtc.h_strike_ratio * initial_spot
    knockin_strike2 = mtc.l_strike_ratio * initial_spot
    knocked_in = mtc.knock_in_date != date.max
    sales_fee = mtc.sales_fee
    snowball = SwhySnowballKnockIn(
        underlying=Security(mtc.code), begin_date=begin_date, observation_dates=observation_dates,
        knockout_barrier=knockout_barrier, initial_spot=initial_spot, rebate_rate=rebate_rate,
        premium_rate=premium_rate, knockin_barrier=knockin_barrier, knockin_strike=knockin_strike,
        knockin_strike2=knockin_strike2, knocked_in=knocked_in, sales_fee=sales_fee)
    option_position = Position(quantity=quantity * mtc.pos, instrument=snowball)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _snowball_step_konck_in(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    begin_date = mtc.begin_date
    observation_dates = mtc.ob_ser_date
    initial_spot = mtc.initial_s
    knockout_barrier = mtc.h_touch_ratio * initial_spot
    knockout_barrier_step = mtc.rebate3
    rebate_rate = mtc.rebate
    premium_rate = mtc.pre_pay_fee * mtc.pre_pay_fee_revenue + (mtc.premium / mtc.p_ratio) * mtc.pos
    knockin_barrier = mtc.l_touch_ratio * initial_spot
    knockin_strike = mtc.h_strike_ratio * initial_spot
    knockin_strike2 = mtc.l_strike_ratio * initial_spot
    knocked_in = mtc.knock_in_date != date.max
    sales_fee = mtc.sales_fee
    snowball = SwhySnowballStepKnockIn(
        underlying=Security(mtc.code), begin_date=begin_date, observation_dates=observation_dates,
        knockout_barrier=knockout_barrier, knockout_barrier_step=knockout_barrier_step,
        initial_spot=initial_spot, rebate_rate=rebate_rate, premium_rate=premium_rate,
        knockin_barrier=knockin_barrier, knockin_strike=knockin_strike, knockin_strike2=knockin_strike2,
        knocked_in=knocked_in, sales_fee=sales_fee)
    option_position = Position(quantity=quantity * mtc.pos, instrument=snowball)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _snowball_sypz(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    begin_date = mtc.begin_date
    observation_dates = mtc.ob_ser_date
    initial_spot = mtc.initial_s
    knockout_barrier = mtc.h_touch_ratio * initial_spot
    knockout_barrier_step = mtc.rebate3
    rebate_rate = mtc.rebate
    premium_rate = mtc.pre_pay_fee * mtc.pre_pay_fee_revenue + (mtc.premium / mtc.p_ratio) * mtc.pos
    knockin_barrier = mtc.l_touch_ratio * initial_spot
    knockin_strike = mtc.h_strike_ratio * initial_spot
    knockin_strike2 = mtc.l_strike_ratio * initial_spot
    knocked_in = mtc.knock_in_date != date.max
    sales_fee = mtc.sales_fee
    snowball = SwhySnowballSypz(
        underlying=Security(mtc.code), begin_date=begin_date, observation_dates=observation_dates,
        knockout_barrier=knockout_barrier, knockout_barrier_step=knockout_barrier_step,
        initial_spot=initial_spot, rebate_rate=rebate_rate, premium_rate=premium_rate,
        knockin_barrier=knockin_barrier, knockin_strike=knockin_strike, knockin_strike2=knockin_strike2,
        knocked_in=knocked_in, sales_fee=sales_fee)
    option_position = Position(quantity=quantity * mtc.pos, instrument=snowball)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _autocall_up(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    initial_spot = mtc.initial_s
    quantity = mtc.actual_not_amt / initial_spot
    observation_dates = mtc.ob_ser_date
    barrier = mtc.h_touch_ratio * initial_spot
    rebate = mtc.rebate * initial_spot
    fee = mtc.premium * mtc.pos / mtc.p_ratio
    settle_ratios = [((d - mtc.begin_date).days + (mtc.deal_date - mtc.end_date).days) / mtc.maturity
                     for d in observation_dates]
    autocall = SwhyAutocall(
        underlying=Security(mtc.code), observation_dates=observation_dates, barrier_direction=BarrierDirection.UP,
        barrier=barrier, rebate=rebate, fee=fee, settle_ratios=settle_ratios)
    option_position = Position(quantity=quantity * mtc.pos, instrument=autocall)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _autocall_down(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    initial_spot = mtc.initial_s
    quantity = mtc.actual_not_amt / initial_spot
    observation_dates = mtc.ob_ser_date
    barrier = mtc.l_touch_ratio * initial_spot
    rebate = mtc.rebate * initial_spot
    fee = mtc.premium * mtc.pos / mtc.p_ratio
    settle_ratios = [((d - mtc.begin_date).days + (mtc.deal_date - mtc.end_date).days) / mtc.maturity
                     for d in observation_dates]
    autocall = SwhyAutocall(
        underlying=Security(mtc.code), observation_dates=observation_dates, barrier_direction=BarrierDirection.DOWN,
        barrier=barrier, rebate=rebate, fee=fee, settle_ratios=settle_ratios)
    option_position = Position(quantity=quantity * mtc.pos, instrument=autocall)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _autocall_up_step(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    initial_spot = mtc.initial_s
    quantity = mtc.actual_not_amt / initial_spot
    observation_dates = mtc.ob_ser_date
    barrier = mtc.h_touch_ratio * initial_spot
    barrier_step = mtc.rebate3 * initial_spot
    rebate = mtc.rebate * initial_spot
    fee = mtc.premium * mtc.pos / mtc.p_ratio
    settle_ratios = [((d - mtc.begin_date).days + (mtc.deal_date - mtc.end_date).days) / mtc.maturity
                     for d in observation_dates]
    autocall = SwhyAutocallStep(
        underlying=Security(mtc.code), observation_dates=observation_dates, barrier_direction=BarrierDirection.UP,
        barrier=barrier, barrier_step=barrier_step, rebate=rebate, fee=fee, settle_ratios=settle_ratios)
    option_position = Position(quantity=quantity * mtc.pos, instrument=autocall)
    return CategorizedCashFlows(option=[option_position], premium=_premium_flows(mtc), exercise_fee=[])


def _airbag(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt / mtc.initial_s
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    p_ratio1 = mtc.p_ratio1
    p_ratio2 = mtc.p_ratio1
    p_ratio3 = mtc.p_ratio2
    barrier = mtc.l_touch_ratio * mtc.initial_s
    strike1 = mtc.h_strike_ratio * mtc.initial_s
    strike2 = mtc.h_touch_ratio * mtc.initial_s
    strike3 = mtc.h_strike_ratio * mtc.initial_s
    airbag = SwhyAirbag(underlying=Security(mtc.code),
                        strike1=strike1, strike2=strike2, strike3=strike3,
                        p_ratio1=p_ratio1, p_ratio2=p_ratio2, p_ratio3=p_ratio3,
                        barrier=barrier, knock_in_date=mtc.knock_in_date,
                        expiration_date=expiration_date, delivery_date=delivery_date)

    position = Position(quantity=quantity * mtc.pos, instrument=airbag)
    return CategorizedCashFlows(option=[position], premium=_premium_flows(mtc), exercise_fee=[])


def _range_accrual(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    quantity = mtc.actual_not_amt * mtc.p_ratio
    expiration_date = mtc.end_date
    delivery_date = mtc.deal_date
    obs_dates = []
    t = mtc.begin_date
    fixings = {}
    while t <= mtc.end_date:
        obs_dates.append(t)
        if t <= as_of:
            fixings[t] = quotes(mtc.code, t)
        t = embedded_calendar().next(t)
    lower_barrier = mtc.l_strike_ratio * mtc.initial_s
    upper_barrier = mtc.h_strike_ratio * mtc.initial_s
    rebate = mtc.rebate
    ra = SwhyRangeAccrual(underlying=Security(mtc.code), expiration_date=expiration_date,
                          lower_barrier=lower_barrier, upper_barrier=upper_barrier,
                          rebate=rebate, obs_dates=obs_dates, delivery_date=delivery_date, fixings=fixings)
    position = Position(quantity=quantity * mtc.pos, instrument=ra)
    return CategorizedCashFlows(option=[position], premium=_premium_flows(mtc), exercise_fee=[])


_product_map = {
    '欧式看涨': _vanilla_european_call,
    '欧式看跌': _vanilla_european_put,
    '美式看涨': _vanilla_american_call,
    '美式看跌': _vanilla_american_put,
    '欧式二元看涨': _digital_call,
    '欧式二元看跌': _digital_put,
    '美式二元看涨': _american_digital_call,
    '美式二元看跌': _american_digital_put,
    '参与式看涨': _participatory_call,
    '看涨价差': _bull_spread,
    '看跌价差': _bear_spread,
    '单向鲨鱼鳍看涨': _ko_call,
    '单向鲨鱼鳍看跌': _ko_put,
    '双向鲨鱼鳍': _double_shark_fin,
    '三层阶梯看涨': _double_digital_call,
    '欧式单向鲨鱼鳍看涨': _shark_fin_terminal_call,
    '欧式单向鲨鱼鳍看跌': _shark_fin_terminal_put,
    '区间保护': _risk_reversal_variant,
    '二元凸式': _convex,
    '雪球看涨自动敲出赎回': _snowball_konck_in,
    '逐步调整雪球看涨自动敲出赎回': _snowball_step_konck_in,
    '收益凭证雪球': _snowball_sypz,
    '看涨自动敲出赎回': _autocall_up,
    '看跌自动敲出赎回': _autocall_down,
    '逐步调整看涨自动敲出赎回': _autocall_up_step,
    '安全气囊': _airbag,
    '区间累积': _range_accrual,
    '自动赎回看涨价差': _ko_call_participatory
}


def cash_flows(mtc: MTCRow, as_of: date, quotes: Callable[[str, date], float]) -> CategorizedCashFlows:
    return _settled_flows(mtc) if _is_settled(mtc, as_of) else _product_map[mtc.option_type](mtc, as_of, quotes)
