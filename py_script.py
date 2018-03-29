# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 16:12:19 2018

@author: xiaoziliang_sx
"""

import cx_Oracle
import pandas as pd
import numpy as np
username="wind"
userpwd="wind"
dsn = '''(DESCRIPTION =
         (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.115)(PORT = 1531))
         (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.116)(PORT = 1531))
         (LOAD_BALANCE = yes)
         (CONNECT_DATA =
          (SERVER = DEDICATED)
          (SERVICE_NAME = windsrv)
          (FAILOVER_MODE =
           (TYPE = SELECT)
           (METHOD = BASIC)
           (RETRIES = 180)
           (DELAY = 5))))'''
connection=cx_Oracle.connect(username, userpwd, dsn) 
print('------------------fetching data------------------------')
sql = "select * from AShareTradingSuspension where S_DQ_RESUMPDATE >= 20150101 AND S_DQ_RESUMPDATE < 20161231 ORDER BY S_DQ_RESUMPDATE" 
resump = pd.read_sql(sql,con = connection)

sql = "select * from AIndexMembers where S_INFO_WINDCODE = '000905.SH' AND (S_CON_OUTDATE >= 20150101 OR CUR_SIGN = 1)" 
member_500etf = pd.read_sql(sql,con = connection)
print('----------------fetching finished--------------------')

resump_member = resump[resump['S_INFO_WINDCODE'].isin(member_500etf['S_CON_WINDCODE'])]
re_new = resump_member.copy()
for ind,dt in enumerate(resump_member['S_DQ_RESUMPDATE']):   
    rows = member_500etf[member_500etf['S_CON_WINDCODE'] == resump_member[ind:ind+1]["S_INFO_WINDCODE"].item()]
    sig = 0
    for ind_,row in rows.iterrows():
        if row['CUR_SIGN'] == 1 and row['S_CON_INDATE']<= dt:
            sig = 1
            continue            
        elif row['CUR_SIGN'] != 1 and row['S_CON_INDATE']<= dt <= row['S_CON_OUTDATE']:
            sig = 1
            continue     
        else:
            pass
    if sig == 1:
        continue
    else:
        re_new.drop(resump_member.index[ind],inplace = True)
        

#%%



from WindPy import *
import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt
w.start()  



#bg = '2017-01-04'+" 09:00:00" 
#ed = '2017-01-04'+" 15:00:00" 
#rst2 = w.wsd("000547.SZ", "pre_close,lastradeday_s", bg[:10], ed[:10], "PriceAdj=F")
#rst = w.wsi("000547.SZ", "close,open,volume,amt", bg, ed, "BarSize=1;PriceAdj=F")
#data=pd.DataFrame(data={'close':rst.Data[0],'open':rst.Data[1],'volume':rst.Data[2],'amt':rst.Data[3]},index=rst.Times)
#preclose = rst2.Data[0][0]



# why 20000000    185*10000*0.12/100 where 0.12 is the 385th of 500 

def FetchData():
    raw = pd.read_csv('highfreqdata_2015.csv',index_col = 0).fillna(method = 'bfill')
    data = {}
    PreClose = {}
    for code,df_code in raw.groupby('code'):
        data[code] = df_code[:242]
        tdt = df_code.index[0][:8]
        bg = tdt[:4]+'-'+tdt[4:6]+'-'+tdt[6:8]
        ed = bg
        print(code,bg)
        rst = w.wsd(code,"pre_close,lastradeday_s",bg,ed, "PriceAdj=F")
        PreClose[code] = rst.Data[0][0]
        
    return data,PreClose



def getResumpDate(re_new,code):
    resumpdate = []
    for c in code:
        resumpdate.append(re_new[re_new['S_INFO_WINDCODE'] == c].iloc[0].S_DQ_RESUMPDATE)    
    return resumpdate



def AMAC(CODE,re_new):
    username="wind"
    userpwd="wind"
    dsn = '''(DESCRIPTION =
             (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.115)(PORT = 1531))
             (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.116)(PORT = 1531))
             (LOAD_BALANCE = yes)
             (CONNECT_DATA =
              (SERVER = DEDICATED)
              (SERVICE_NAME = windsrv)
              (FAILOVER_MODE =
               (TYPE = SELECT)
               (METHOD = BASIC)
               (RETRIES = 180)
               (DELAY = 5))))'''
    connection=cx_Oracle.connect(username, userpwd, dsn) 
    Sustime = pd.DataFrame(index = CODE)
    Suspend  = {}
    print('------------------fetching data------------------------')
    for code in CODE:
        resumpdate = re_new.loc[re_new['S_INFO_WINDCODE'] == code,'S_DQ_RESUMPDATE'].values[0]
        sql = "select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE,S_DQ_PCTCHANGE,S_DQ_TRADESTATUS from AShareEODPrices where S_INFO_WINDCODE = '"+code+"' AND TRADE_DT < "+resumpdate+" ORDER BY TRADE_DT"
        Suspend[code] = pd.read_sql(sql,con = connection) 
        sp = Suspend[code]
        suspenddate = sp[sp['S_DQ_TRADESTATUS'] != '停牌'].iloc[-1,1]
        enddate = sp.iloc[-1,1]
        print(code)
        Sustime.loc[code,'suspenddate'] = suspenddate
        Sustime.loc[code,'enddate'] = enddate
    print('-------------------finished------------------------')
    
    return Suspend,Sustime    
   
    
def getAMAC():
    username="wind"
    userpwd="wind"
    dsn = '''(DESCRIPTION =
             (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.115)(PORT = 1531))
             (ADDRESS = (PROTOCOL = TCP)(HOST = 10.81.3.116)(PORT = 1531))
             (LOAD_BALANCE = yes)
             (CONNECT_DATA =
              (SERVER = DEDICATED)
              (SERVICE_NAME = windsrv)
              (FAILOVER_MODE =
               (TYPE = SELECT)
               (METHOD = BASIC)
               (RETRIES = 180)
               (DELAY = 5))))'''
    connection=cx_Oracle.connect(username, userpwd, dsn)    
    sql = "select S_INFO_WINDCODE, S_CON_WINDCODE from AIndexMembersCITICS"
    AMACallcode = pd.read_sql(sql,con = connection) 
    return AMACallcode



def AMACfunc2(Sustime,AMACallcode):
#    tdt = Sustime.suspenddate.min()
#    bg = tdt[:4]+'-'+tdt[4:6]+'-'+tdt[6:8]
#    ed = '2017-12-31'
    
    #rst = w.wsd('000905.SH',"close,pre_close,lastradeday_s",bg,ed, "PriceAdj=F")
    #CSI500=pd.DataFrame(data={'close':rst.Data[0],'pre_close':rst.Data[1],'lastradeday_s':rst.Data[2]},index=rst.Times)
    
    for code in Sustime.index:
        bgg = Sustime.loc[code,'suspenddate']
        edd = Sustime.loc[code,'enddate']
        BG = dt.date(year = int(bgg[:4]),month = int(bgg[4:6]),day = int(bgg[6:8]))
        ED = dt.date(year = int(edd[:4]),month = int(edd[4:6]),day = int(edd[6:8]))
        amaccode = AMACallcode.loc[AMACallcode.S_CON_WINDCODE == code,'S_INFO_WINDCODE'].values[0]
        rst =  w.wsd(amaccode,"close",BG,ED, "PriceAdj=F")
        print(amaccode)
        amac = pd.DataFrame(data={'close':rst.Data[0]},index=rst.Times)
        rt = (amac.loc[ED,'close']-amac.loc[BG,'close'])/amac.loc[BG,'close']
        Sustime.loc[code,'AMACrt'] = rt
        
    return Sustime


def getSecName(code):
    secname = []
    for c in code:
        rst = w.wss(c, "sec_name","")
        secname.append(rst.Data[0][0])

    return secname

def func1(data):
    vp1 = np.array([])
    vp2 = np.array([])
    for code in data.keys():       
        vol1 = data[code][:12].volume.sum() 
        vol2 = data[code][-11:].volume.sum()
        vp1 = np.append(vp1,(vol1+vol2)/data[code].volume.sum())
        vol3 = data[code][:17].volume.sum()
        vp2 = np.append(vp2,vol3/data[code].volume.sum())
    
    return vp1.mean(),vp2.mean()



def EmpiricalAlgo1(data,code,preclose,direction = 'Buy'):
    
    fstop = data.loc[data.index[0],'close']
    if direction == 'Buy':
        if fstop <= preclose:  
            dsel = data[:17].copy()
            dsel.loc[dsel.close <= preclose,'amttr'] = dsel[dsel.close<=preclose].amount*0.3
            dsel.loc[dsel.close > preclose,'amttr'] = dsel[dsel.close >preclose].amount*0.0
            dsel.loc[dsel.close <= preclose,'voltr'] = dsel[dsel.close<=preclose].volume*0.3
            dsel.loc[dsel.close > preclose,'voltr'] = dsel[dsel.close >preclose].volume*0.0            
            
        else:
            dsel = pd.concat([data[:12],data[-11:]],axis = 0)
            dsel['amttr'] = dsel.amount*0.3
            dsel['voltr'] = dsel.volume*0.3
            
    elif direction == 'Sell':
        if fstop >= preclose:  
            dsel = data[:17].copy()
            dsel.loc[dsel.close >= preclose,'amttr'] = dsel[dsel.close>=preclose].amount*0.3
            dsel.loc[dsel.close < preclose,'amttr'] = dsel[dsel.close <preclose].amount*0.0
            dsel.loc[dsel.close >= preclose,'voltr'] = dsel[dsel.close>=preclose].volume*0.3
            dsel.loc[dsel.close < preclose,'voltr'] = dsel[dsel.close <preclose].volume*0.0            
            
        else:
            dsel = pd.concat([data[:12],data[-11:]],axis = 0)
            dsel['amttr'] = dsel.amount*0.3
            dsel['voltr'] = dsel.volume*0.3        
            
            
    dmean, vol, amt = Postprocess(dsel)         
    return dsel, dmean, vol, amt            
            
        


def EmpiricalTWAP(data,code,preclose,direction = 'Buy'):
    fstop = data.loc[data.index[0],'close']
    if direction == 'Buy':
        if fstop <= preclose: 
            dsel = data[:17].copy()
            dsel.loc[dsel.close <= preclose,'amttr'] = dsel[dsel.close<=preclose].amount*0.3
            dsel.loc[dsel.close > preclose,'amttr'] = dsel[dsel.close >preclose].amount*0.0
            dsel.loc[dsel.close <= preclose,'voltr'] = dsel[dsel.close<=preclose].volume*0.3
            dsel.loc[dsel.close > preclose,'voltr'] = dsel[dsel.close >preclose].volume*0.0                 
            
        else:
            dsel = data.copy()
            dsel.loc[dsel.index < dsel.index[12],'amttr'] = dsel.loc[dsel.index < dsel.index[12],'amount']*0.3
            dsel.loc[dsel.index < dsel.index[12],'voltr'] = dsel.loc[dsel.index < dsel.index[12],'volume']*0.3          
            amttwap = dsel.iloc[:12]['amount'].sum()*0.3/len(dsel.iloc[12:])
            dsel.loc[dsel.index >= dsel.index[12],'amttr'] = amttwap
            voltwap = dsel.iloc[:12]['volume'].sum()*0.3/len(dsel.iloc[12:])
            dsel.loc[dsel.index >= dsel.index[12],'voltr'] = voltwap
    elif direction == 'Sell':
        if fstop >= preclose:  
            dsel = data[:17].copy()
            dsel.loc[dsel.close >= preclose,'amttr'] = dsel[dsel.close>=preclose].amount*0.3
            dsel.loc[dsel.close < preclose,'amttr'] = dsel[dsel.close <preclose].amount*0.0
            dsel.loc[dsel.close >= preclose,'voltr'] = dsel[dsel.close>=preclose].volume*0.3
            dsel.loc[dsel.close < preclose,'voltr'] = dsel[dsel.close <preclose].volume*0.0            
            
        else:
            dsel = data.copy()
            dsel.loc[dsel.index < dsel.index[12],'amttr'] = dsel.loc[dsel.index < dsel.index[12],'amount']*0.3
            dsel.loc[dsel.index < dsel.index[12],'voltr'] = dsel.loc[dsel.index < dsel.index[12],'volume']*0.3 
            amttwap = dsel.iloc[:12]['amount'].sum()*0.3/len(dsel.iloc[12:])
            dsel.loc[dsel.index >= dsel.index[12],'amttr'] = amttwap
            voltwap = dsel.iloc[:12]['volume'].sum()*0.3/len(dsel.iloc[12:])
            dsel.loc[dsel.index >= dsel.index[12],'voltr'] = voltwap       
    
    dmean, vol, amt = Postprocess(dsel)         
    return dsel, dmean, vol, amt            


def EmpiricalVWAP(data,code,preclose,direction = 'Buy'):
    fstop = data.loc[data.index[0],'close']
    if direction == 'Buy':
        if fstop <= preclose: 
            dsel = data[:17].copy()
            dsel.loc[dsel.close <= preclose,'amttr'] = dsel[dsel.close<=preclose].amount*0.3
            dsel.loc[dsel.close > preclose,'amttr'] = dsel[dsel.close >preclose].amount*0.0
            dsel.loc[dsel.close <= preclose,'voltr'] = dsel[dsel.close<=preclose].volume*0.3
            dsel.loc[dsel.close > preclose,'voltr'] = dsel[dsel.close >preclose].volume*0.0                 
            
        else:
            dsel = data.copy()
            dsel['amttr'] = dsel.amount*0.3
            dsel['voltr'] = dsel.volume*0.3   
            L = dsel.iloc[12:]['volume'].sum()/dsel.iloc[:12]['volume'].sum()
            dsel.loc[dsel.index >= dsel.index[12],'amttr'] = dsel.loc[dsel.index >= dsel.index[12],'amttr']/L
            dsel.loc[dsel.index >= dsel.index[12],'voltr'] = dsel.loc[dsel.index >= dsel.index[12],'voltr']/L
            
            
            
    elif direction == 'Sell':
        if fstop >= preclose:  
            dsel = data[:17].copy()
            dsel.loc[dsel.close >= preclose,'amttr'] = dsel[dsel.close>=preclose].amount*0.3
            dsel.loc[dsel.close < preclose,'amttr'] = dsel[dsel.close <preclose].amount*0.0
            dsel.loc[dsel.close >= preclose,'voltr'] = dsel[dsel.close>=preclose].volume*0.3
            dsel.loc[dsel.close < preclose,'voltr'] = dsel[dsel.close <preclose].volume*0.0            
            
        else:
            dsel = data.copy()
            dsel['amttr'] = dsel.amount*0.3
            dsel['voltr'] = dsel.volume*0.3   
            L = dsel.iloc[12:]['volume']/dsel.iloc[:12]['volume']
            dsel.loc[dsel.index >= dsel.index[12],'amttr'] = dsel.loc[dsel.index >= dsel.index[12],'amttr']/L
            dsel.loc[dsel.index >= dsel.index[12],'voltr'] = dsel.loc[dsel.index >= dsel.index[12],'voltr']/L   
    
    dmean, vol, amt = Postprocess(dsel)         
    return dsel, dmean, vol, amt            
         
            

#def EmpiricalTWAP(data,code,preclose,direction = 'buy'):
#    ### rule: trade from open to close by one-minute-level's amt
#

def PrecloseAlgo(data,code,preclose,direction = 'Buy'):
    ### rule: trade from open to close by one-minute-level's amt
    
    amt = np.array([])
    voltr = np.array([])
    for ind in data.index[1:]:
        if data.shift(1).loc[ind,'close'] <= preclose:
            if direction == 'Buy':
                amt = np.append(amt,data.shift(1).loc[ind,'amount']*0.3)
                voltr = np.append(voltr,data.shift(1).loc[ind,'volume']*0.3) 
            elif direction == 'Sell':
                amt = np.append(amt,data.shift(1).loc[ind,'amount']*0.5*0.3)
                voltr = np.append(voltr,data.shift(1).loc[ind,'volume']*0.5*0.3) 
        else:
            if direction == 'Buy':
                amt = np.append(amt,data.shift(1).loc[ind,'amount']*0.5*0.3)
                voltr = np.append(voltr,data.shift(1).loc[ind,'volume']*0.5*0.3) 
            elif direction == 'Sell':   
                amt = np.append(amt,data.shift(1).loc[ind,'amount']*0.3)
                voltr = np.append(voltr,data.shift(1).loc[ind,'volume']*0.3) 
        
        
        if amt.sum() >= data.amount.sum()/3*0.3:
            dsel = data[data.index <= ind][1:].copy() 
            dsel['voltr'] = voltr
            dsel['amttr'] = amt            
            break  
        
    dmean, vol, amt = Postprocess(dsel)         
    return dsel, dmean, vol, amt
    


def Postprocess(data):
    dmean =(data.close*data.voltr).sum()/data.voltr.sum()
    vol = data.voltr.sum()
    amt = data.amttr.sum()       
    return dmean,vol,amt        
    
    



if __name__ == '__main__':
    
    alldf,PreClose = FetchData() 
    Code = list(alldf.keys())
    
    
    
    #---------------------------------
    Suspend,Sustime = AMAC(Code,re_new)
    AMACallcode = getAMAC()
    Sustime = AMACfunc2(Sustime,AMACallcode)
    secname = getSecName(Code)
    resumpdate = getResumpDate(re_new,Code)
    preclose = [(lambda c: PreClose[c])(c) for c in PreClose.keys()]
    op = [(lambda c: alldf[c].iloc[0,1])(c) for c in PreClose.keys()]
    cl = [(lambda c: alldf[c].iloc[-1,2])(c) for c in PreClose.keys()]
    rt = ((np.array(cl)- np.array(preclose))/np.array(preclose)*100).round(2)
    
    
    mx = pd.DataFrame({'name':np.array(secname),
                  '停牌日':Sustime['suspenddate'].values,
                  '复牌日':np.array(resumpdate),
                  '昨收':np.array(preclose).round(2),
                  '今开':np.array(op).round(2),
                  '今收':np.array(cl).round(2),
                  '收益率%':rt,
                  '同期行业指数收益率%':(Sustime['AMACrt'].values*100).round(2),                 
                  },index = Code)
    
    mx.to_csv("明细.csv")
    
    
    
    
    
    
    Mean = []
    Em1dmean =[];Em1vol = [];Em1amt = []
    PCdmean =[];PCvol = [];PCamt = []
    Tdmean = [];Tvol = [];Tamt = []
    Vdmean = [];Vvol = [];Vamt = []
    #Code = []
    Codem = []
    
    #for code in alldf.keys():
    for code in Sustime[Sustime.AMACrt <= -0.05].index:
        #Code.append(code)
        Codem.append(code)
        dsel, dmean, vol, amt = EmpiricalAlgo1(alldf[code],code,PreClose[code],direction='Buy') 
        Em1dmean.append(dmean)
        Em1vol.append(vol)
        Em1amt.append(amt)        
        
        dsel,dmean,vol,amt = PrecloseAlgo(alldf[code],code,PreClose[code],direction='Buy')
        PCdmean.append(dmean)
        PCvol.append(vol)
        PCamt.append(amt)
        
        dsel,dmean,vol,amt = EmpiricalTWAP(alldf[code],code,PreClose[code],direction='Buy')
        Tdmean.append(dmean)
        Tvol.append(vol)
        Tamt.append(amt)   

        dsel,dmean,vol,amt= EmpiricalVWAP(alldf[code],code,PreClose[code],direction='Buy')
        Vdmean.append(dmean)
        Vvol.append(vol)
        Vamt.append(amt)         
        
        dmean =(alldf[code].close*alldf[code].volume).sum()/alldf[code].volume.sum()
        Mean.append(dmean)
    

#####  Buy operation
####   about a quater of stocks traindg can not be finished in one business day  (296-212)/2 = 42 42/171= 24.5%
        
##### Sell direction
####   about a quater of stocks traindg can not be finished in one business day  (257-235)/2 = 11 11/171= 6.43%      

