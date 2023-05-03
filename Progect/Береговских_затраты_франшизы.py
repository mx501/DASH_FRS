import os
import pandas as pd
import xlsxwriter
import gc
import numpy as np
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

PUT = "C:\\Users\\lebedevvv\\Desktop\\Для юли\\"
class RENAME:
    def Rread(self):
        print("Загрузка справочника магазинов...")
        replacements = pd.read_excel("https://docs.google.com/spreadsheets/d/1SfuC2zKUFt6PQOYhB8EEivRjy4Dz-o4WDL-IR7CT3Eg/export?exportFormat=xlsx")
        """replacements = pd.read_excel(PUT + "Справочники\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")"""
        unique_vals = set(replacements ['НАЙТИ'].unique())
        return unique_vals
class FLOAT:
    def float_colms(self, name_data, name_col):
        for i in name_col:
            name_data[i] = (name_data[i].astype(str)
                                              .str.replace("\xa0", "")
                                              .str.replace(",", ".")
                                              .fillna("0")
                                              .astype("float")
                                              .round(2))
        return name_data
    """Для нескольких столбцов"""
    def float_colm(self, name_data, name_col):

        name_data[name_col] = (name_data[name_col].astype(str)
                                          .str.replace("\xa0", "")
                                          .str.replace(",", ".")
                                          .fillna("0")
                                          .astype("float")
                                          .round(2))
        return name_data
    """для одного столбца"""

# обороты за период дебет
file = PUT + "01.03.2023.xlsx"

zatrat_frandhiza = pd.read_excel(file)
zatrat_frandhiza = zatrat_frandhiza[["Оборотно-сальдовая ведомость по счету 44 за Март 2023 г.", "Unnamed: 3"]].reset_index(drop=True)

# создание новой строки с названиями столбцов
zatrat_frandhiza_001 = pd.DataFrame(zatrat_frandhiza.iloc[0]).T
zatrat_frandhiza_001.columns = ['Дробить',  "Обороты за период"]
zatrat_frandhiza = zatrat_frandhiza[9:]
zatrat_frandhiza.columns = ['Дробить',  "Обороты за период"]
# объединение данных

zatrat_frandhiza = pd.concat([zatrat_frandhiza_001, zatrat_frandhiza]).reset_index(drop=True)
Spisok_magaz = RENAME().Rread()

len_Spisok_magaz = len(zatrat_frandhiza)

for i in Spisok_magaz:
    zatrat_frandhiza.loc[zatrat_frandhiza['Дробить'] == i, 'магазин'] = zatrat_frandhiza['Дробить']

zatrat_frandhiza["магазин"] = zatrat_frandhiza["магазин"].ffill()

zatrat_frandhiza.loc[zatrat_frandhiza["магазин"] == zatrat_frandhiza['Дробить'], ["магазин","Обороты за период",'Дробить']]= [np.nan,np.nan,np.nan]
zatrat_frandhiza = zatrat_frandhiza.loc[zatrat_frandhiza["магазин"].notnull()]
zatrat_frandhiza["дата"] = os.path.basename(file[:-5])
zatrat_frandhiza = zatrat_frandhiza.fillna(0)

FLOAT().float_colm(name_data=zatrat_frandhiza,name_col="Обороты за период")


zatrat_frandhiza = zatrat_frandhiza.pivot_table (index = ['магазин',"дата"], columns = 'Дробить', values= 'Обороты за период').reset_index()
zatrat_frandhiza = zatrat_frandhiza.drop(columns={"Итого"})
zatrat_frandhiza["ЗАТРАТЫ НА МАРКЕТИНГ"] = zatrat_frandhiza["Дегустации"] + zatrat_frandhiza["Музыкальное сопровождение"]+\
                                           zatrat_frandhiza["Подарок покупателю (сервисная фишка)"]\
                                           + zatrat_frandhiza["Расходы на полиграфию (наклейки, ценники и пр)"]\
                                           + zatrat_frandhiza["Маркетинг"]


zatrat_frandhiza.to_csv(PUT + "↓Затраты обработанный.csv", encoding="ANSI", sep=';',
                         index=False, decimal=',')




# убираем многоуровневые названия столбцов
#zatrat_frandhiza.columns = [f"{col[0]} {col[1]}" for col in zatrat_frandhiza.columns]

print(zatrat_frandhiza[:50])
