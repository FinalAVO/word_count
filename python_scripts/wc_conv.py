import pandas as pd
import sys

filename = sys.argv[1]

data= pd.read_csv("/word_count/result/" + filename, sep='\t').sort_values(by=['1'], ascending=False, ignore_index=True)

dict_ = {
        '0': 'text',
        '1': 'frequency'
        }

data.rename(columns=dict_, inplace=True)

if len(data) >= 20:
    data = data[:20]
    data.to_csv("/word_count/result/" + filename, encoding='utf-8-sig', index=False)
else:
    data.to_csv("/word_count/result/" + filename, encoding='utf-8-sig', index=False)

