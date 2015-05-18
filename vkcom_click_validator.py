__author__ = 'intsco'

import threading
import os
import numpy as np
import pandas as pd
import gzip


class ClickValidator(threading.Thread):
    def __init__(self, callback=None, *args, **kwargs):
        super(ClickValidator, self).__init__(*args, **kwargs)
        self.callback = callback

        base_path = os.path.dirname(os.path.realpath(__file__)) + '/data/'
        # self.data_fn = base_path + 'click_data.csv'
        self.data_fn = base_path + 'click_data.gz'
        self.report_fn = base_path + 'report.csv'

    def run(self):
        self.validate()
        self.callback('DONE')

    def status(self):
        return 'IN PROGRESS'

    def get_column_dict(self, ser):
        return dict(map(lambda (x,y): (y,x), enumerate(ser.unique())))

    def get_time_span(self, start, end):
        return max((end - start).total_seconds() / 60, 1)

    def sliding_window_clicks_n(self, tss):
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

    def get_user_penalties(self, ser, asc):
        pen_dict = {}
        prev, penalty = None, 0
        for i, (u, v) in enumerate(ser.order(ascending=asc).iterkv()):
            if v != prev:
                penalty, prev = i, v
            pen_dict[u] = penalty
        return pd.Series(pen_dict)

    def validate(self):
        col_renaming = {'Click time': 'ts',
                        'Ad id': 'ad',
                        'Advertiser id': 'advertiser',
                        'Site id': 'site',
                        'User id': 'user',
                        'User IP': 'ip'}
        f = gzip.open(self.data_fn, 'rb')
        raw_data = pd.read_csv(f, engine='python').rename(columns=col_renaming)

        data = pd.DataFrame()
        data['id'] = raw_data.id
        data['ts'] = raw_data.ts.map(lambda ts: pd.to_datetime(ts, unit='s'))
        for col in raw_data.columns[2:]:
            col_dict = self.get_column_dict(raw_data[col])
            data[col] = raw_data[col].map(col_dict)

        # Users with more than 5 clicks
        user_clicks = data.groupby('user').size()
        data2 = data[data.user.isin(user_clicks[user_clicks > 5].index)].sort(['ip', 'user', 'ts'])

        # Calculate user behaviour metrics
        user_metrics = pd.DataFrame()
        user_metrics['click_n'] = data2.groupby('user').size()
        user_metrics['avg_clicks_per_min'] = data2.groupby('user').apply(
            lambda df: df.shape[0] / self.get_time_span(df.ts.min(), df.ts.max()))
        user_metrics['max_clicks_per_site'] = data2.groupby(['user', 'site']).size().reset_index().groupby('user')[0].max()
        user_metrics['uniq_sites'] = data2.groupby('user').apply(lambda df: df.site.unique().shape[0])
        user_metrics['max_clicks_per_10min'] = data2.groupby('user').apply(
            lambda df: self.sliding_window_clicks_n(df.ts).max())

        # Get user penalties per metric
        user_penalties = user_metrics.copy()
        for col, asc in zip(user_metrics.columns, [True, True, True, False, True]):
            user_penalties[col] = self.get_user_penalties(user_metrics[col], asc)

        # Average penalties and get top N percents of the most suspicious users
        quantile = int(np.round(user_metrics.shape[0]*0.2))
        top20perc_users = user_penalties.mean(axis=1).order(ascending=False).head(quantile)

        # Mark suspicious user clicks with True
        data['suspicious_user'] = data.user.isin(top20perc_users.index)

        data[['id', 'suspicious_user']].to_csv(self.report_fn, index=False)
        # open(self.status_fn, 'w').write('DONE')