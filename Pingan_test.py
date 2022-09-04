from datetime import date
from dataclasses import dataclass
from datetime import datetime
from numpy import int64
import pandas as pd
import xlrd as xlrd
import xlwt as xlwt
import math
import xlwings as xw
from openpyxl.utils.datetime import from_excel

#_na_values = {
#    '100091_C_20210128_01_CALL': ''
#}

data0 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\0.xlsx', 'Sheet1')
data1 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\1000.xlsx', 'Sheet1')
data2 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\2000.xlsx', 'Sheet1')
data3 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\3000.xlsx', 'Sheet1')
data4 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\4000.xlsx', 'Sheet1')
data5 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\5000.xlsx', 'Sheet1')
data6 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\6000.xlsx', 'Sheet1')
data7 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\7000.xlsx', 'Sheet1')
data8 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\8000.xlsx', 'Sheet1')
data9 = pd.read_excel(r'C:\Users\wangr\Desktop\Bachelier\平安BCT\Test\雪球Data\9000.xlsx', 'Sheet1')
data = pd.concat([data0,data1,data2,data3,data4,data5,data6,data7,data8,data9],axis=0,ignore_index=False)
data = data.drop(columns = [datetime(2021, 3, 22, 0, 0)])


starting_day = datetime(2021, 2, 10, 0, 0)
payoff_list = []
k_list = []

for index,row in data.iterrows():
    k = 0

    for i in range(1,219):
        if (row[i] >= 6243.71 
        and (data.columns[i] == datetime(2021, 3, 10, 0, 0)
        or data.columns[i] == datetime(2021, 4, 9, 0, 0)
        or data.columns[i] == datetime(2021, 5, 10, 0, 0)
        or data.columns[i] == datetime(2021, 6, 10, 0, 0)
        or data.columns[i] == datetime(2021, 7, 9, 0, 0)
        or data.columns[i] == datetime(2021, 8, 9, 0, 0)
        or data.columns[i] == datetime(2021, 9, 10, 0, 0)
        or data.columns[i] == datetime(2021, 10, 8, 0, 0)
        or data.columns[i] == datetime(2021, 11, 8, 0, 0)
        or data.columns[i] == datetime(2021, 12, 8, 0, 0)
        or data.columns[i] == datetime(2022, 1, 10, 0, 0)
        or data.columns[i] == datetime(2022, 2, 10, 0, 0)
        )):                                                                #观察日敲出
            KO_days = data.columns[i] - starting_day
            payoff = 0.2 * 4876200 * (KO_days.total_seconds()/24/60/60)/365 / (4876200/6243.71) * math.exp(-0.0425*i/244)
            payoff_list.append(payoff)
            k_list.append(k)
            break
            
        elif row[i] <= 4994.968 and i !=218:
            k = k + 1                                                      #观察日敲入
            #continue
        elif i == 218 and k == 0:                                          #全程未敲入未敲出
            payoff = 0.2 * 4876200 / (4876200/6243.71) * math.exp(-0.0425*i/244)
            payoff_list.append(payoff)
            k_list.append(k)
            
        #elif i != 219:                                                    #目前未敲入未敲出
            #continue
        elif i == 218 and k != 0:                                          #全程敲入未敲出
            payoff = (4876200 * max(-1,min(0,row[i]/6243.71 - 1)) + 0 * 4876200) / (4876200/6243.71) * math.exp(-0.0425*i/244)
            payoff_list.append(payoff)
            k_list.append(k)


f = xlwt.Workbook()
test_result = f.add_sheet('test_result')

c = 0
for r in range(0,10001):
    test_result.write(r,c,payoff_list[r])  

f.save('./result3.xls')

test = pd.read_excel('./result.xls')
test.head()

      
            
        


starting_day
type(KO_days.total_seconds()/24/60/60)
KO_days = data.columns[i] - starting_day  
(KO_days.total_seconds()/24/60/60)
payoff_list
k_list
len(k_list)
payoff
len(payoff_list)
math.exp(-0.0425*i/244)
data.columns[i]
payoff_list[9999]
#a = params['100091_C_20210128_01_CALL'][0]
#b = params['100091_C_20210128_01_CALL'][1]
#c = params['100091_C_20210128_01_CALL'][2]
#d = params['100091_C_20210128_01_CALL'][3]
#e = params['100091_C_20210128_01_CALL'][4]
