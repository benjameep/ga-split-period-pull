import pandas as pd
import numpy as np
import base64
import hashlib
import json
import re
from pathlib import Path
import os

def report_to_frame(report):
    # Get metric names as list
    metric_names = [metric.get('name') for metric in report.get('columnHeader').get('metricHeader').get('metricHeaderEntries')]    
    
    # Get dimension names as list
    dimension_names = report.get('columnHeader').get('dimensions')
    
    # Collect dimension values into a dataframe
    dimensions = [row.get('dimensions') for row in report.get('data').get('rows',[])]
    dimensions = pd.DataFrame(dimensions, columns = dimension_names)
    
    # Create MultiIndex from dimension values
    index = pd.MultiIndex.from_frame(dimensions)
    
    # Collect metric values into list of lists
    rows = []
    num_date_ranges = 1
    for row in report.get('data').get('rows',[]):
        # Concat all metrics for each date range, will seperate later
        values = []
        num_date_ranges = len(row.get('metrics'))
        for date_range in row.get('metrics'):
            values += date_range.get('values')
        rows.append(values)
    
    
    columns = None
    if num_date_ranges > 1:
        # Create column index as (date_ranges x metrics)
        columns = pd.MultiIndex.from_product([range(num_date_ranges), metric_names], names=('date_range','metric'))
    else:
        columns = pd.Index(metric_names)
    
    # Create the DataFrame
    df = pd.DataFrame(rows, index=index, columns=columns)
    
    # Convert to numbers
    df = df.apply(pd.to_numeric, errors='ignore')
    
    return df


def hash_dict(d):
    string = json.dumps(d)
    buffer = str.encode(string)
    hashed = hashlib.sha1(buffer).digest()
    encoded = base64.b64encode(hashed).decode('utf-8')
    clean = re.sub(r'\W','',encoded)
    return clean[0:10]

def run_report(analytics, reqs, cache_folder_name='./data/cache'):
    hashes = [hash_dict(req) for req in reqs]
    filenames = [os.path.join(cache_folder_name,hashed+'.json') for hashed in hashes]
    reports = np.zeros(len(reqs)).tolist()
    requests = []
    idx = []
    
    # Create Directory if doesn't exist
    Path(cache_folder_name).mkdir(parents=True, exist_ok=True)
    
    # Read Response from cache if avaliable
    for i, request in enumerate(reqs):
        if os.path.exists(filenames[i]):
            with open(filenames[i]) as f:
                reports[i] = json.load(f)
        else:
            requests.append(request)
            idx.append(i)
    
    # Get Missing Reports
    if len(requests):
        print('requesting {} report{}...'.format(len(requests), 's' if len(requests) > 1 else ''))
        r = analytics.reports().batchGet(body={'reportRequests':requests}).execute()

        # Write missing reports to cache
        for i, report in zip(idx, r.get('reports')):
            reports[i] = report
            with open(filenames[i],'w') as f:
                json.dump(report, f)
    
    return reports


def each_period(reqs, start, end, period):
    dates = pd.date_range(start=start, end=end, freq=period)
    dates = dates.to_period(dates.freq)
    for date in dates:
        start = date.start_time.strftime('%Y-%m-%d')
        end = date.end_time.strftime('%Y-%m-%d')
        yield [{**r, 'dateRanges':[{'startDate':start, 'endDate':end}]} for r in reqs]

def each_page(analytics, reqs, *args, **kwargs):
    idx = range(len(reqs))
    while len(reqs):
        next_reqs = []
        next_idx = []
        reports = run_report(analytics, reqs, *args, **kwargs)
        for i, request, report in zip(idx, reqs, reports):
            if 'nextPageToken' in report:
                next_reqs.append({**request, 'pageToken': report.get('nextPageToken')})
                next_idx.append(i)
            yield (i,report)
        reqs = next_reqs
        idx = next_idx
