import os
import pandas as pd
PUT= "D:\\Python\\Dashboard\\NEW\\Январь февраль\\"
# в туже паппку , удалить исходник надо

for filename in os.listdir(PUT):
    print(filename)
    if filename.endswith(".txt"):
        df = pd.read_csv(os.path.join(PUT, filename), sep="\t", encoding='utf-8', skiprows=3,  parse_dates=['По дням'],dayfirst=True, names=(
                        ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                         'Себестоимость',
                         'Выручка', 'Прибыль', 'СписРуб', 'Списания, кг']))
        print(filename)
        months = pd.to_datetime(df['По дням'], format='%Y-%m-%d').dt.to_period('M').unique()
        if len(months) == 2:
            df1 = df[pd.to_datetime(df['По дням'], format='%Y-%m-%d').dt.to_period('M') == months[0]]
            df2 = df[pd.to_datetime(df['По дням'], format='%Y-%m-%d').dt.to_period('M') == months[1]]

            filename1 = filename.split(".")[0] + "_" + str(months[0]) + ".txt"
            filename2 = filename.split(".")[0] + "_" + str(months[1]) + ".txt"
            df1.to_csv(PUT +  filename1, encoding='utf-8',
                        sep="\t", index=False)
            df2.to_csv(PUT + filename2, encoding='utf-8',
                       sep="\t", index=False)