
# coding: utf-8

# In[417]:

from __future__ import division
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
get_ipython().magic(u'matplotlib inline')


# #Data Preprocessing

# In[418]:

raw_data = pd.read_csv('clicks_hashed.csv').rename(columns={'Click time': 'ts',
                                                        'Ad id': 'ad', 
                                                        'Advertiser id': 'advertiser',
                                                        'Site id': 'site',
                                                        'User id': 'user',
                                                        'User IP': 'ip'})


# In[419]:

raw_data.head()


# In[420]:

def get_column_dict(series):
    return dict(map(lambda (x,y): (y,x), enumerate(series.unique())))    


# In[421]:

ad_map = get_column_dict(raw_data.ad)
advertiser_map = get_column_dict(raw_data.advertiser)
site_map = get_column_dict(raw_data.site)
user_map = get_column_dict(raw_data.user)
ip_map = get_column_dict(raw_data.ip)


# In[422]:

data = raw_data.copy().sort(['ip', 'user', 'ts'])
data['ts'] = raw_data.ts.map(lambda ts: pd.to_datetime(ts, unit='s'))
data['ad'] = raw_data.ad.map(ad_map)
data['advertiser'] = raw_data.advertiser.map(advertiser_map)
data['site'] = raw_data.site.map(site_map)
data['user'] = raw_data.user.map(user_map)
data['ip'] = raw_data.ip.map(ip_map)


# In[423]:

data.tail(20)


# # Data Description

# In[424]:

#pd.to_datetime(data.ts.min(), unit='s'), pd.to_datetime(data.ts.max(), unit='s')
data.ts.min(), data.ts.max()


# In[425]:

print data.shape
print len(data.ad.unique())
print len(data.advertiser.unique())
print len(data.site.unique())
print len(data.user.unique())
print len(data.ip.unique())


# In[426]:

user_clicks = data.groupby('user').size()
data2 = data[data.user.isin(user_clicks[user_clicks > 5].index)].sort(['ip', 'user', 'ts'])
data2.shape


# #Suspicous User Behaviour Metrics

# In[427]:

# data2.groupby('user').size().order(ascending=False).head(200)


# In[428]:

user_metrics = pd.DataFrame()


# In[429]:

user_metrics['click_n'] = data2.groupby('user').size()


# In[430]:

def get_time_span(start, end):
    return max((end - start).total_seconds() / 60, 1)


# In[431]:

user_metrics['avg_clicks_per_min'] = data2.groupby('user').apply(
    lambda df: df.shape[0] / get_time_span(df.ts.min(), df.ts.max()))


# In[432]:

# plt.scatter(user_metrics.click_n,
#             user_metrics.clicks_per_min)


# In[433]:

user_metrics['max_clicks_per_site'] = data2.groupby(['user', 'site']).size().reset_index().groupby('user')[0].max()


# In[434]:

#user_metrics.sort('max_clicks_per_site', ascending=False)


# In[435]:

user_metrics['uniq_sites'] = data2.groupby('user').apply(lambda df: df.site.unique().shape[0])


# In[436]:

# df = data2[data2.user == 121628]
# df


# In[440]:

def sliding_window_clicks_n(tss):
    window_events_n = []
    ws = 10
    window = []
    for ts in tss.order():
#         print ts
        window.append(ts)
        if len(window) > 0:
            i, n, wnd_start = 0, len(window), 0
            while True:
                if i < n:
                    if (window[n-1] - window[i]).total_seconds()/60 > ws:
                        wnd_start = i+1
                        i += 1
                    else:
                        break
            del window[:wnd_start]
#             print window
            window_events_n.append(len(window))

    return pd.Series(window_events_n)


# In[ ]:




# In[457]:

# pd.Series(sliding_window_clicks_n(data2[data2.user == 36].ts)).plot(kind='bar')
# pd.Series(sliding_window_clicks_n(data2[data2.user == 22078].ts)).plot(kind='bar')
# pd.Series(sliding_window_clicks_n(data2[data2.user == 740].ts)).plot(kind='bar')
# pd.Series(sliding_window_clicks_n(data2[data2.user == 181172].ts)).plot(kind='bar')
pd.Series(sliding_window_clicks_n(data2[data2.user == 93795].ts)).plot(kind='bar')


# In[445]:

user_metrics['max_clicks_per_10m_slot'] = data2.groupby('user').apply(
    lambda df: sliding_window_clicks_n(df.ts).max())


# In[462]:

user_metrics.sort('max_clicks_per_10m_slot', ascending=False).head(10)


# In[501]:

def get_penalties(ser, asc):
    #print ser
    pen_dict = {}
    prev = None
    for i,(u,v) in enumerate(ser.order(ascending=asc).iterkv()):
        if v != prev:
            penalty, prev = i, v
        pen_dict[u] = penalty
    return pd.Series(pen_dict)


# In[519]:

user_penalties = user_metrics.copy()
for col, asc in zip(user_metrics.columns, [True, True, True, False, True]):
    user_penalties[col] = get_user_penalties(user_metrics[col], asc)
print user_penalties.shape
user_penalties


# In[534]:

quantile = int(np.round(user_metrics.shape[0]*0.2))
print quantile
top20perc_users = user_penalties.mean(axis=1).order().tail(quantile)
top20perc_users.index


# In[549]:

data['suspicious_user'] = data.user.isin(top20perc_users.index)


# In[552]:

print data['suspicious_user'].sum() / data['suspicious_user'].shape[0]
print data.shape
data['suspicious_user'].head()


# # Interclick Span Distribution

# In[270]:

def get_intervals(tss):
    n = len(tss)-1
    tss2 = tss.copy()
    tss2.sort('ts')
    span1 = tss2.iloc[1:]
    span2 = tss2.iloc[:-1]
    return [(span1.iloc[i] - span2.iloc[i]).total_seconds()/60 for i in range(n)]


# In[281]:

#df = data2[data2.user == 16060]
click_intervals = pd.Series(np.hstack(data2.groupby('user').apply(lambda df: get_intervals(df.ts)).values))
click_intervals.head()


# In[464]:

plt.hist(click_intervals[click_intervals<60].values, bins=120);


# In[286]:

click_intervals.shape[0], click_intervals[click_intervals<10].shape[0]


# In[ ]:




# In[ ]:




# In[ ]:




# #Suspicous IP Behaviour Metrics

# In[295]:

data2.groupby('ip').apply(lambda df: df.user.unique().shape[0]).order(ascending=False).head(10)


# In[294]:

data2[data2.ip == 517].sort(['user', 'ts'])


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



