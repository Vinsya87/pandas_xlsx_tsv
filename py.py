import pandas as pd

data = {'Manager': ['Юрий', 'Оля', 'Юрий', 'Оля', 'Юрий'],
        'Sales': [100, 200, 300, 400, 500]}
df = pd.DataFrame(data)

grouped = df.groupby('Manager').agg({'Sales': ['count', 'sum']})

result_dict = {}
for index, row in grouped.iterrows():
    result_dict[index] = [row[('Sales', 'count')], row[('Sales', 'sum')]]

print(result_dict)
#Ответ: {'Оля': [2, 600], 'Юрий': [3, 900]}
