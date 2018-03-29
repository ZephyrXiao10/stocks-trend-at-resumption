# -*- coding: utf-8 -*-
"""
Created on Thu Mar  8 14:39:55 2018

@author: xiaoziliang_sx
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns


#x = np.arange(0.0, 2, 0.01)
#y1 = np.sin(2 * np.pi * x)
#y2 = 1.2 * np.sin(4 * np.pi * x)
#fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True)
#
#ax1.fill_between(x, 0, y1)
#ax1.set_ylabel('between y1 and 0')
#
#ax2.fill_between(x, y1, 1)
#ax2.set_ylabel('between y1 and 1')
#
#ax3.fill_between(x, y1, y2)
#ax3.set_ylabel('between y1 and y2')
#ax3.set_xlabel('x')


x = np.array(Code)
y0 = np.array(Mean)
y1 = np.array(Em1dmean)
y2 = np.array(PCdmean)
y3 = np.array(Tdmean)
y4 = np.array(Vdmean)

#fig = plt.gcf()
##fig.set_size_inches(12.8, 6.52)
#
#line1 = plt.plot(x,np.log(y1/y0), label = "BOLL, sharpe: ")
#line2 = plt.plot(x,np.log(y2/y0), label = "BOLLBAN, sharpe:")
#line3 = plt.plot(x,np.log(y3/y0), label = "CK, sharpe:")
#line4 = plt.plot(x,np.log(y4/y0), label = "GMA, sharpe:")
#
##ling11 = plt.plot(dt.index,((-data[0]+data[1]+data[2]+data[3]+data[4]+data[5]+data[6]+data[7]+data[8])/8).values,label = "all within b&h")
#plt.legend()
#plt.xlabel("date")
#plt.ylabel("PnL")
#
#plt.xticks(rotation=90,size = 6)
#
#
#
#
#
#
#
#fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)
#fig.subplots_adjust(left=0.2, wspace=0.6)
##----------------------------------------------------------
#y = np.log(y1/y0)
##fig,ax = plt.subplot()
##plt.plot(x, y1, x, y2, color='black')
#ax1.plot(x,y)
#ax1.set_title('Empirical Algo vs Mean')
#ax1.ylabel('log(TWAP mean/ Empr mean)')
##ax1.xticks(rotation=90,size = 6)
#
#
##-----------------------------------------------------------
#y = np.log(y2/y0)
##fig,ax = plt.subplot()
##plt.plot(x, y1, x, y2, color='black')
#ax2.plot(x,y,color = 'red')
#ax2.grid()
#ax2.title('PCdmean Algo vs Mean')
#ax2.ylabel('log(VWAP mean/ Empr mean)')
#ax2.xticks(rotation=90,size = 6)
#
#y = np.log(y3/y0)
##fig,ax = plt.subplot()
##plt.plot(x, y1, x, y2, color='black')
#ax3.plot(x,y)
#ax3.grid()
#ax3.title('TWAP Algo vs Mean')
#ax3.ylabel('log(TWAP mean/ Empr mean)')
#ax3.xticks(rotation=90,size = 6)
#
#
##-----------------------------------------------------------
#y = np.log(y4/y0)
##fig,ax = plt.subplot()
##plt.plot(x, y1, x, y2, color='black')
#ax4.plot(x,y,color = 'red')
#ax4.grid()
#ax4.title('VWAP Algo vs Mean')
#ax4.ylabel('log(VWAP mean/ Empr mean)')
#ax4.xticks(rotation=90,size = 6)
#







y = np.log(y1/y0)
ymean = y.mean().round(4)
quantile = []
qnum = [25,50,75]
for num in qnum:
    quantile.append(format(np.percentile(y,num),'1f'))

sns.set(color_codes=True)
g = sns.distplot(y);

ttl= 'mean='+str(ymean)+'\n'+'25p='+str(quantile[0])+', '+'50p='+str(quantile[1])+', '+'75p='+str(quantile[2])
g.set_title(ttl,size = 17)
g.set_xlabel('Empirical Algo Distribution',size = 17)




y = np.log(y2/y0)
ymean = y.mean().round(4)
quantile = []
qnum = [25,50,75]
for num in qnum:
    quantile.append(format(np.percentile(y,num),'1f'))

sns.set(color_codes=True)
g = sns.distplot(y);

ttl= 'mean='+str(ymean)+'\n'+'25p='+str(quantile[0])+', '+'50p='+str(quantile[1])+', '+'75p='+str(quantile[2])
g.set_title(ttl,size = 17)
g.set_xlabel('PresentClose Algo Distribution',size = 17)


y = np.log(y3/y0)
ymean = y.mean().round(4)
quantile = []
qnum = [25,50,75]
for num in qnum:
    quantile.append(format(np.percentile(y,num),'1f'))

sns.set(color_codes=True)
g = sns.distplot(y);

ttl= 'mean='+str(ymean)+'\n'+'25p='+str(quantile[0])+', '+'50p='+str(quantile[1])+', '+'75p='+str(quantile[2])
g.set_title(ttl,size = 17)
g.set_xlabel('TWAP Algo Distribution',size = 17)



y = np.log(y4/y0)
ymean = y.mean().round(4)
quantile = []
qnum = [25,50,75]
for num in qnum:
    quantile.append(format(np.percentile(y,num),'1f'))

sns.set(color_codes=True)
g = sns.distplot(y);

ttl= 'mean='+str(ymean)+'\n'+'25p='+str(quantile[0])+', '+'50p='+str(quantile[1])+', '+'75p='+str(quantile[2])
g.set_title(ttl,size = 17)
g.set_xlabel('VWAP Algo Distribution',size = 17)






#----------------------------------------------------------------------mean

fig, ax = plt.subplots()
ind = np.array([u[9:] for u in alldf['000008.SZ'].index])
series = pd.Series(0.,index = ind)
CO = Codem
for code in CO:
    series = series + (alldf[code].close/alldf[code].close[0]).values
series = series/len(CO)    
    
line = ax.plot(series, label = "Depreciated stocks' trends(mean) in 1st resumption date")    
ax.legend()
for label in ax.get_xticklabels():
    if label in ax.get_xticklabels()[1::20]:
        label.set_visible(True)
    else:
        label.set_visible(False)
plt.xlabel("date")
plt.ylabel("PnL")

plt.xticks(rotation=45,size =10)



#----------------------------------------------------------------------medium

fig, ax = plt.subplots()
ind = np.array([u[9:] for u in alldf['000021.SZ'].index])
median_df = pd.DataFrame(index = ind)
CO = Codem
for code in CO:
    median_df[code] = (alldf[code].close/alldf[code].close[0]).values
    
med = median_df.median(1)
    
line = ax.plot(med, label = "Depreciated stocks' trends(median) in 1st resumption date")    
ax.legend()
for label in ax.get_xticklabels():
    if label in ax.get_xticklabels()[1::20]:
        label.set_visible(True)
    else:
        label.set_visible(False)
plt.xlabel("date")
plt.ylabel("PnL")

plt.xticks(rotation=45,size =10)




#----------------------------------------------------------------------


fig, ax = plt.subplots()
ind = np.array([u[9:] for u in alldf['000021.SZ'].index])
series = pd.Series(0.,index = ind)
volume = pd.Series(0.,index = ind)
#pc_series = pd.Series(0.,index = ind)
for code in Code:
    series = series + (alldf[code].close/alldf[code].close[0]).values
    volume = volume + (alldf[code].volume/alldf[code].volume[0]).values
    #pc_series = pc_series+ PreClose[code]/alldf[code].close[0]

    
bar = ax2
ax.legend()
for label in ax.get_xticklabels():
    if label in ax.get_xticklabels()[1::20]:
        label.set_visible(True)
    else:
        label.set_visible(False)
plt.xticks(rotation=45,size =10)
plt.xlabel("date")
plt.ylabel("PnL")


ax2 = ax.twinx()
sns.set_color_codes("pastel")



line = ax.plot(series, label = "ALL stocks' trends in 1st resumption date")  
#sns.barplot(x = volume.index,y=volume.values,
#            label="Total", color="b",ax = ax2)   


#line = ax.plot(series, label = "ALL stocks' trends in 1st resumption date")  



#----------------------------------------------------------------------


fig, ax = plt.subplots()
ind = np.array([u[9:] for u in alldf['000021.SZ'].index])
series = pd.Series(0.,index = ind)
volume = pd.Series(0.,index = ind)
#pc_series = pd.Series(0.,index = ind)
for code in Code:
    series = series + (alldf[code].close/alldf[code].close[0]).values
    volume = volume + (alldf[code].volume/alldf[code].volume[0]).values
    #pc_series = pc_series+ PreClose[code]/alldf[code].close[0]


ax2 = ax.twinx()  
#sns.set_color_codes("pastel") 
bar = ax2.bar(x = volume.index,height=volume.values, color="b")

line = ax.plot(series, label = "ALL stocks' trends in 1st resumption date")  
ax.legend()
for label in ax2.get_xticklabels():
    if label in ax2.get_xticklabels()[1::20]:
        label.set_visible(True)
    else:
        label.set_visible(False)
plt.xticks(rotation=45,size =10)
plt.xlabel("date")
plt.ylabel("PnL")







line = ax.plot(series, label = "ALL stocks' trends in 1st resumption date")  
#sns.barplot(x = volume.index,y=volume.values,
#            label="Total", color="b",ax = ax2)   


#line = ax.plot(series, label = "ALL stocks' trends in 1st resumption date")  

