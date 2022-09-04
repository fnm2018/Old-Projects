# -*- coding: utf-8 -*-
from bct.analytics.priceable.feature import BarrierDirection
import math
from datetime import date
from typing import Sequence, Union

import numpy as np
from bct.analytics.pycore.daycount import bus_250_embedded

YearDays = 250


def singlemytdate(t1: date, t2: date, rest: float = 0) -> float:
    if t2 >= t1:
        return bus_250_embedded(t1, t2) + rest / YearDays
    return -bus_250_embedded(t2, t1) - rest / YearDays


def my_date_diff(t1: date, t2: Union[date, np.ndarray], varargin: str = 'N') -> Union[float, np.ndarray]:
    # 如果t1>t2那么返回值为0
    if isinstance(t2, date):
        if varargin == 'T':
            return max(0, bus_250_embedded(t1, t2) + 1 / YearDays)
        else:
            return max(0, ((t2 - t1).days + 1) / 365)
    if varargin == 'T':
        t_interval = np.array([bus_250_embedded(t1, t) + 1 / YearDays for t in t2])
    else:
        t_interval = np.array([((t - t1).days + 1) / 365 for t in t2])
    return t_interval * (t_interval > 0)


# 雪球看涨自动敲出赎回
def AutoCall_SnowBall_Prob_first(S: float, K: float, U: float, T: date, sigma: float, r: float, dividend: float,
                                 Rebate: float, C: float, par: float, ObDate: Sequence[date], BeginDate: date,
                                 life: float = 1, rest: float = 0) -> float:
    n = 500
    L = 0.05 * par  # 取一个较小的低障碍价来近似无向下障碍的情形

    ObDate = sorted(ObDate)
    TotalDays = (ObDate[-1] - BeginDate).days + 1
    TotalTime = TotalDays / 365
    ObSeri = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate)))
    num = max(sum([ObSeri[i] > 0 for i in range(len(ObSeri))]), 1)
    SettleRatio = np.array(list(map(lambda x: ((x - BeginDate).days + rest * YearDays) * 1.0 / TotalDays,
                                    ObDate[-num:]))) * TotalTime  # 票息的系数
    time_ = np.array(list(map(lambda x: ((x - T).days + rest * YearDays) * 1.0 / TotalDays, ObDate[-num:])))  # 折现天数
    ObSeri = ObSeri[-num:]
    TimeRatio = ((ObDate[-1] - T).days + rest * YearDays) / 365
    dt = np.array([ObSeri[i+1] - ObSeri[i] for i in range(len(ObSeri) - 1)])
    if ObSeri[0] > 0 or (ObSeri[0] == 0 and num == 1):
        dt = np.insert(dt, 0, ObSeri[0])

    c = (r - dividend - 0.5 * sigma ** 2) * dt[1:]
    cc = (r - dividend - 0.5 * sigma ** 2) * dt[0] - (np.log(L / S) + np.log(U / L)/(2 * n))
    d = np.log(U / L) / n
    k1 = np.linspace(0, n - 1, n)
    k2 = np.linspace(-n + 1, n - 1, 2 * n - 1)

    def f(x): return 0.5 * math.erf(((x - 0.5) * d - cc)/(sigma * np.sqrt(2 * dt[0])))
    p = np.array(list(map(f, k1 + 1))) - np.array(list(map(f, k1)))
    q = np.zeros((len(dt) - 1, 2 * len(p) - 1))
    for i in range(len(dt) - 1):
        def g(x): return 0.5 * math.erf(((x - 0.5) * d - c[i])/(sigma * np.sqrt(2 * dt[1+i])))
        q[i] = np.array(list(map(g, k2 + 1))) - np.array(list(map(g, k2)))

    b = np.zeros((num, len(p)))
    b[0] = p
    for i in range(num - 1):
        aux_matrix = np.zeros((len(p), len(p)))
        for j in range(len(p)):
            aux_matrix[:, j] = q[i, j:j + len(p)][::-1].T
        b[i + 1] = np.dot(b[i], aux_matrix)

    prob_in = np.sum(b, axis=1)
    prob_out = np.insert(prob_in[0:-1] - prob_in[1:], 0, 1-np.sum(p))

    df = np.exp(-r * time_*TotalTime)
    price_fix = np.sum(par * df * (Rebate - C) * SettleRatio * prob_out) \
        - C * par * SettleRatio[-1] * df[-1] * (1 - np.sum(prob_out))

    j_0 = int(np.log(K / L) / d + 0.5)
    Put_Seri = L * np.exp((0.5 + np.linspace(0, j_0 - 1, j_0)) * d)
    put_payoff = K - Put_Seri
    put_payoff[put_payoff > 0.5 * K] = 0.5 * K
    price_UP = np.exp(-r * TimeRatio) * np.sum(b[-1, :j_0] * put_payoff)
    if TimeRatio == 0:
        price_UP = max(K - S, 0)
    price = price_fix - price_UP
    return price


def AutoCall_SnowBall_Prob_second(S: float, K: float, L: float, U: float, T: date, sigma: float, r: float,
                                  dividend: float, Rebate: float, par: float, out_idx: int, ObDate: Sequence[date],
                                  BeginDate: date, life: float = 1, rest: float = 0) -> float:
    # outidx为0代表未敲出，正常计算价值；为1代表敲出，本部分的价值为0
    if out_idx != 0:
        return 0
    n = 500
    U_in_fre = 2 * K

    ObSeri = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate)))
    num = max(sum([ObSeri[i] > 0 for i in range(len(ObSeri))]), 1)
    ObDate = sorted(ObDate)
    ObDate = ObDate[-num:]
    SettleRatio = ((ObDate[-1] - BeginDate).days + 1) / 365
    TimeRatio = ((ObDate[-1] - T).days + rest * YearDays) / 365
    TotalStep = math.ceil(max(singlemytdate(T, ObDate[-1], rest * YearDays) * YearDays, 1))
    Out_Obs = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate))) * YearDays
    Out_Obs = list(map(math.ceil, Out_Obs))
    dt = np.ones(2) / YearDays
    if rest > 0 or (rest == 0 and T == ObDate[-1]):
        dt[0] = rest

    c = (r - dividend - 0.5 * sigma ** 2) * dt[1]
    cc = (r - dividend - 0.5 * sigma ** 2) * dt[0] - (np.log(L / S) + np.log(U_in_fre / L) / (2 * n))
    d = np.log(U_in_fre / L) / n
    k1 = np.linspace(0, n - 1, n)
    k2 = np.linspace(-n + 1, n - 1, 2 * n - 1)
    i_0 = int(np.log(U / L) / d + 0.5)

    def f(x): return 0.5 * math.erf(((x - 0.5) * d - cc) / (sigma * np.sqrt(2 * dt[0])))
    p = np.array(list(map(f, k1 + 1))) - np.array(list(map(f, k1)))
    def g(x): return 0.5 * math.erf(((x - 0.5) * d - c) / (sigma * np.sqrt(2 * dt[1])))
    q = np.array(list(map(g, k2 + 1))) - np.array(list(map(g, k2)))

    b = np.zeros((TotalStep, len(p)))
    b[0] = p[:]
    aux_matrix = np.zeros((len(p), len(p)))

    for i in range(len(p)):
        aux_matrix[:, i] = q[i:i + len(p)][::-1].T
    for i in range(TotalStep - 1):
        b[i + 1] = np.dot(b[i], aux_matrix)
        if (i + 2) in Out_Obs:
            b[i + 1, i_0:] = 0
    if TotalStep == 1:
        b[0, i_0:] = 0
    prob_in = np.sum(b, axis=1)

    j_0 = int(np.log(K / L) / d + 0.5)
    Put_Seri = L * np.exp((0.5 + np.linspace(0, j_0 - 1, j_0)) * d)
    price_Binary = par * Rebate * SettleRatio * np.exp(-r * TimeRatio) * prob_in[-1]
    put_payoff = K - Put_Seri
    put_payoff[put_payoff > 0.5 * K] = 0.5 * K
    price_UDO = np.exp(-r * TimeRatio) * np.sum(b[-1, :j_0] * put_payoff)
    if TimeRatio == 0:
        price_UDO = max(K - S, 0)
    return -price_UDO - price_Binary


def AutoCall_SnowBall_All(S: float, K: float, L: float, U: float, T: date, sigma: float, r: float, dividend: float,
                          Rebate: float, C: float, par: float, outidx: int, ObDate: Sequence[date], BeginDate: date,
                          rest: float = 0, life: float = 1) -> float:
    return AutoCall_SnowBall_Prob_first(S, K, U, T, sigma, r, dividend, Rebate,
                                        C, par, ObDate, BeginDate, life, rest) \
        - AutoCall_SnowBall_Prob_second(S, K, L, U, T, sigma, r, dividend, Rebate,
                                        par, outidx, ObDate,  BeginDate, life, rest)


# 逐步调整雪球看涨自动敲出赎回
def AutoCall_SnowBall_Step_first(S: float, K: float, U: float, M: float, Step: float, T: date, sigma: float, r: float,
                                 dividend: float, Rebate: float, C: float, par: float, ObDate: Sequence[date],
                                 BeginDate: date, SalesFee: float, rest: float = 0) -> float:
    n = 1000
    L = 0.05 * par  # 取一个较小的低障碍价来近似无向下障碍的情形

    ObDate = sorted(ObDate)
    TotalDays = (ObDate[-1] - BeginDate).days
    TotalTime = TotalDays / 365
    ObSeri = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate)))
    num = max(sum([ObSeri[i] > 0 for i in range(len(ObSeri))]), 1)
    U_Seri = (U + np.array(range(len(ObDate))) * Step * par)[-num:]
    SettleRatio = np.array(list(map(lambda x: ((x - BeginDate).days + rest * YearDays) * 1.0 / TotalDays,
                                    ObDate[-num:]))) * TotalTime  # 计算票息的系数
    time_ = np.array(list(map(lambda x: ((x - T).days + rest * YearDays) * 1.0 / TotalDays, ObDate[-num:])))  # 折现天数
    ObSeri = ObSeri[-num:]
    TimeRatio = ((ObDate[-1] - T).days + rest * YearDays) / 365
    dt = np.array([ObSeri[i+1] - ObSeri[i] for i in range(len(ObSeri)-1)])
    if ObSeri[0] > 0 or (ObSeri[0] == 0 and num == 1):
        dt = np.insert(dt, 0, ObSeri[0])

    c = (r - dividend - 0.5 * sigma ** 2) * dt[1:]
    cc = (r - dividend - 0.5 * sigma ** 2) * dt[0] - (np.log(L / S) + np.log(U_Seri[0] / L)/(2 * n))

    d = np.log(U_Seri[0] / L) / n
    k1 = np.linspace(0, n - 1, n)
    k2 = np.linspace(-n + 1, n - 1, 2 * n - 1)

    def f(x): return 0.5 * math.erf(((x - 0.5) * d - cc)/(sigma * np.sqrt(2 * dt[0])))
    p = np.array(list(map(f, k1 + 1))) - np.array(list(map(f, k1)))

    q = np.zeros((len(dt) - 1, 2 * len(p) - 1))
    for i in range(len(dt)-1):
        def g(x): return 0.5 * math.erf(((x - 0.5) * d - c[i])/(sigma * np.sqrt(2 * dt[1+i])))
        q[i] = np.array(list(map(g, k2 + 1))) - np.array(list(map(g, k2)))

    b = np.zeros((num, len(p)))
    b[0] = p
    for i in range(num - 1):
        aux_matrix = np.zeros((len(p), len(p)))
        i_0 = int(np.log(U_Seri[i+1] / L) / d + 0.5)
        for j in range(len(p)):
            aux_matrix[:, j] = q[i, j:j + len(p)][::-1].T
        b[i + 1] = np.dot(b[i], aux_matrix)
        b[i+1, i_0:] = 0

    prob_in = np.sum(b, axis=1)
    prob_out = np.insert(prob_in[0:-1] - prob_in[1:], 0, 1 - np.sum(p))

    df = np.exp(-r * time_ * TotalTime)
    price_fix = np.sum(par * df * (Rebate - C) * SettleRatio * prob_out) \
        - C * SettleRatio[-1] * par * df[-1] * (1 - np.sum(prob_out))
    j_0 = min(int(np.log(K / L) / d + 0.5), n)
    Put_Seri = L * np.exp((0.5 + np.linspace(0, j_0 - 1, j_0)) * d)
    price_UP = np.exp(-r * TimeRatio) * np.sum(
        b[-1, :j_0] * (np.min(np.array([K - Put_Seri, np.ones(len(Put_Seri)) * (K - M)]), axis=0)
                       - par * SalesFee))
    if TimeRatio == 0:
        price_UP = min(max(K - S, 0), K - M)
    return price_fix - price_UP


def AutoCall_SnowBall_Step_second(S: float, K: float, L: float, U: float, M: float, Step: float, T: date, sigma: float,
                                  r: float, dividend: float, Rebate: float, par: float, out_idx: int,
                                  ObDate: Sequence[date], BeginDate: date, SalesFee: float,
                                  rest: float = 0) -> float:
    # outidx为0代表未敲出，正常计算价值；为1代表敲出，本部分的价值为0
    if out_idx == 1:
        return 0

    ObSeri = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate)))
    num = max(sum([ObSeri[i] > 0 for i in range(len(ObSeri))]), 1)
    ObDate = sorted(ObDate)
    U_Seri = (U + np.array(range(len(ObDate))) * Step * par)[-num:]
    ObDate = ObDate[-num:]
    SettleRatio = ((ObDate[-1] - BeginDate).days) / 365
    TimeRatio = ((ObDate[-1] - T).days + rest * YearDays) / 365
    TotalStep = math.ceil(max(singlemytdate(T, ObDate[-1], rest * YearDays) * YearDays, 1)) + 1
    Out_Obs = np.array(list(map(lambda x: singlemytdate(T, x, rest * YearDays), ObDate))) * YearDays
    Out_Obs = np.ceil(Out_Obs) + 1
    dt = np.ones(2) / YearDays
    if rest > 0 or (rest == 0 and T == ObDate[-1]):
        dt[0] = rest

    n = 1000
    U_in_fre = 2 * U_Seri[0]
    d = np.log(U_in_fre / L) / n
    i_0 = int(np.log(U_Seri[0] / L) / d + 0.5) + 1

    d = np.log(U_Seri[0] / L) / i_0
    c = (r - dividend - 0.5 * sigma ** 2) * dt[0]
    cc = (r - dividend - 0.5 * sigma ** 2) * dt[1] - (np.log(L / S) + np.log(L*np.exp(n*d-d) / L) / (2 * n))

    k1 = np.linspace(0, n - 1, n)
    k2 = np.linspace(-n + 1, n - 1, 2 * n - 1)

    def f(x): return 0.5 * math.erf(((x - 0.5) * d - cc) / (sigma * np.sqrt(2 * dt[0])))
    p = np.array(list(map(f, k1 + 1))) - np.array(list(map(f, k1)))
    def g(x): return 0.5 * math.erf(((x - 0.5) * d - c) / (sigma * np.sqrt(2 * dt[1])))
    q = np.array(list(map(g, k2 + 1))) - np.array(list(map(g, k2)))

    b = np.zeros((TotalStep, len(p)))
    b[0] = p[:]
    aux_matrix = np.zeros((len(p), len(p)))
    count = 0
    if TotalStep == 1 or Out_Obs[0] == 1:
        b[0, i_0:] = 0
    for i in range(len(p)):
        aux_matrix[:, i] = q[i:i + len(p)][::-1].T
    for i in range(TotalStep - 1):
        b[i + 1] = np.dot(b[i], aux_matrix)
        if (i + 2) in Out_Obs:
            i_0 = int(np.log(U_Seri[count] / L) / d + 0.5)
            b[i + 1, i_0:] = 0
            count += 1

    prob_in = np.sum(b, axis=1)
    j_0 = int(np.log(K / L) / d + 0.5)
    Put_Seri = L * np.exp((0.5 + np.linspace(0, j_0 - 1, j_0)) * d)
    price_Binary = par * Rebate * SettleRatio * np.exp(-r * TimeRatio) * prob_in[-1]
    price_UDO = np.exp(-r * TimeRatio) * np.sum(
        b[-1, :j_0] * (np.min(np.array([K - Put_Seri, np.ones(len(Put_Seri)) * (K - M)]), axis=0)
                       - par * SalesFee))
    if TimeRatio == 0:
        price_UDO = min(max(K - S, 0), (K - M))
    return -price_UDO - price_Binary


def AutoCall_SnowBall_Step_All(S: float, K: float, L: float, U: float, M: float, Step: float, T: date, sigma: float,
                               r: float, dividend: float, Rebate: float, C: float, par: float, outidx: int,
                               ObDate: Sequence[date], BeginDate: date, SalesFee: float, rest: float = 0) -> float:
    return AutoCall_SnowBall_Step_first(S, K, U, M, Step, T, sigma, r, dividend, Rebate,
                                        C, par, ObDate, BeginDate, SalesFee, rest) \
        - AutoCall_SnowBall_Step_second(S, K, L, U, M, Step, T, sigma, r, dividend, Rebate,
                                        par, outidx, ObDate,  BeginDate, SalesFee, rest)


# 看涨自动敲出赎回, 看跌自动敲出赎回, 逐步调整看涨自动敲出赎回
def AutoCallSwapGreeksPDE(S: float, K: Union[float, Sequence[float]], TDate: date, sigma: float, r: float,
                          dividend: float, Rebate: float, Fee: float, ObDate: Sequence[date],
                          SettleRatio: Sequence[float], barrier_direction: BarrierDirection) -> float:
    # S表示标的价格，K表示行权价格，sigma表示波动率，r表示无风险利率，dividend表示分红率，Rebate表示观察日触碰所获得的收益
    # TDate表示估值日当天的日期
    # ObDate表示观察日序列
    if TDate >= ObDate[-1]:
        if (barrier_direction == BarrierDirection.UP and S >= K) \
                or (barrier_direction == BarrierDirection.DOWN and S <= K):
            return Rebate - Fee
        return 0

    NumInDay = 10
    YearDays = 250
    ObDate = np.array(ObDate)
    idx_obs_start = np.sum(ObDate < TDate)
    SettleRatio = SettleRatio[idx_obs_start:]
    ObDate = ObDate[idx_obs_start:]
    ObSeri = (my_date_diff(TDate, ObDate, 'T') * YearDays - 1) * NumInDay + 1
    ObSeri = np.round(ObSeri).astype(int)
    M = ObSeri[-1] - 1
    MT = my_date_diff(TDate, ObDate[-1], 'T')
    dt = 1 / (YearDays * NumInDay)
    Numdx = 500
    cof = 0.5
    b = r - dividend
    var = sigma**2
    mu = b - var / 2
    Sx = np.log(S)
    K = np.array(K)
    if K.size == 1:
        K = np.ones(len(ObDate)) * K
    logK = np.log(K)
    UpBound = max(Sx, np.max(logK)) + max(mu, 0) * MT + 5 * sigma * MT**0.5
    DownBound = min(Sx, np.min(logK)) - max(mu, 0) * MT - 5 * sigma * MT**0.5
    dx = (UpBound - DownBound) / Numdx
    XArray = np.linspace(UpBound, DownBound, Numdx + 1)
    SXIndex = np.sum(XArray > Sx)
    XArray = XArray + (Sx - XArray[SXIndex])
    w = var * dt / dx**2
    b = 1 + cof * dt * r + cof * w
    a = (w + mu * dt / dx) / 2 * cof
    c = (w - mu * dt / dx) / 2 * cof
    bb = 1 - (1 - cof) * dt * r - (1 - cof) * w
    aa = (w + mu * dt / dx) / 2 * (1 - cof)
    cc = (w - mu * dt / dx) / 2 * (1 - cof)
    Vt = np.diag(np.ones(Numdx + 1) * b) - np.diag(np.ones(Numdx) * a, -1) - np.diag(np.ones(Numdx) * c, 1)
    VtNext = np.diag(np.ones(Numdx + 1) * bb) + np.diag(np.ones(Numdx) * aa, -1) + np.diag(np.ones(Numdx) * cc, 1)

    ferr = np.zeros(Numdx + 1)
    if barrier_direction == BarrierDirection.UP:
        ferr[-1] = -c * Fee - cc * Fee
        HArray = Rebate * (XArray >= logK[-1]) - Fee
        idx_bound = 0
    else:
        ferr[0] = -c * Fee - cc * Fee
        HArray = Rebate * (XArray <= logK[-1]) - Fee
        idx_bound = -1

    def BoundaryValue(tw: int) -> float:
        TempIndex = np.sum(ObSeri < tw)
        if tw in ObSeri:
            return (Rebate - Fee) * SettleRatio[TempIndex]
        else:
            ResidualT = (ObSeri[TempIndex] - tw) / NumInDay / YearDays
            return (Rebate - Fee) * SettleRatio[TempIndex] * np.exp(-r * ResidualT)

    VtInverse = np.linalg.inv(Vt)
    idx_obs = len(ObSeri) - 2
    CArray = HArray
    for i in range(M, 0, -1):
        ferr[idx_bound] = (a * BoundaryValue(i) + aa * BoundaryValue(i+1))
        CArray = VtInverse @ (VtNext @ HArray + ferr)
        if i in ObSeri:
            if barrier_direction == BarrierDirection.UP:
                CArray = (Rebate-Fee) * SettleRatio[idx_obs] * (XArray >= logK[idx_obs]) \
                    + CArray * (XArray < logK[idx_obs])
            else:
                CArray = (Rebate-Fee) * SettleRatio[idx_obs] * (XArray <= logK[idx_obs]) \
                    + CArray * (XArray > logK[idx_obs])
            idx_obs -= 1
        HArray = CArray

    return CArray[SXIndex]
