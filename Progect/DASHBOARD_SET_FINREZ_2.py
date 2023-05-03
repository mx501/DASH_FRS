
import os
import pandas as pd
import gc
import numpy as np
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

PUT = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\"



class RENAME:
    def Rread(self, name_data, name_col):
        print("Загрузка справочника магазинов...")
        replacements = pd.read_excel("https://docs.google.com/spreadsheets/d/1SfuC2zKUFt6PQOYhB8EEivRjy4Dz-o4WDL-IR7CT3Eg/export?exportFormat=xlsx")
        """replacements = pd.read_excel(PUT + "Справочники\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")"""
        rng = len(replacements)
        for i in range(rng): name_data[name_col] = \
            name_data[name_col].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
        return name_data
    """функция переименование"""
class FLOAT:
    def float_colms(self, name_data, name_col , name):
        for i in name_col:
            print("Форматирование столбцов в формат FLOAT: " + name + ": " + i )
            name_data[i] = (name_data[i].astype(str)
                                              .str.replace("\xa0", "")
                                              .str.replace(",", ".")
                                              .fillna("0")
                                              .astype("float")
                                              .round(2))
        return name_data
    """Для нескольких столбцов"""
    def float_colm(self, name_data, name_col):
        name_data[name_col ] = (name_data[name_col ].astype(str)
                                          .str.replace("\xa0", "")
                                          .str.replace(",", ".")
                                          .fillna("0")
                                          .astype("float")
                                          .round(2))
        return name_data


class Finrez:
    def Finrez(self):

        print(
            "Обновление финреза\n")
        for files in os.listdir(PUT + "Финрез\\Исходник\\"):
            FINREZ = pd.read_excel(PUT + "Финрез\\Исходник\\" + files, sheet_name="Динамика ТТ исходник")
            FINREZ = FINREZ.rename(columns={"Торговая точка": "магазин", "Дата": "дата",
                                            "Канал": "канал",
                                            "Режим налогообложения": "режим налогообложения",
                                            "Канал на последний закрытый период": "канал на последний закрытый период"})
            FINREZ = RENAME().Rread(name_data=FINREZ, name_col="магазин")

            FINREZ = FINREZ.reset_index(drop=True)
            FINREZ = FINREZ.loc[FINREZ['дата'] >= "2022-01-01"]

            #FINREZ = FINREZ.loc[(FINREZ["магазин"] == "Роялти ФРС") | (FINREZ["магазин"] == "Офис")]
            FINREZ = FINREZ[[ "дата","магазин","* Прибыль (+) / Убыток (-) (= Т- ОЕ)  БЕЗ РОЯЛТИ ФРС"]]
            FLOAT().float_colm(name_data=FINREZ,name_col="* Прибыль (+) / Убыток (-) (= Т- ОЕ)  БЕЗ РОЯЛТИ ФРС")
            print(FINREZ)
            FINREZ.to_csv(PUT + "Финрез\\Данные для ДШ\\↓Финрез_прибыль без роялти.csv", encoding="ANSI", sep=';',
                              index=False, decimal=',')
    def Finrez_prognozniy(self):
        for files in os.listdir(PUT + "Финрез\\Прогнозный\\"):
            FINREZ = pd.read_excel(PUT + "Финрез\\Прогнозный\\" + files, sheet_name="Апрель 23")

            #Прибыль (до вычета роялти)
            FINREZ = FINREZ[["Апрель с 1 по","Unnamed: 43",]]
            FINREZ["дата"] = ('2023-04-01')
            FINREZ["дата"] = FINREZ["дата"].astype("datetime64[ns]")

            # удаление первых трех строк
            data = FINREZ.iloc[3:].reset_index(drop=True)

            # создание новой строки с названиями столбцов
            FINREZ_001 = pd.DataFrame(data.iloc[0]).T
            FINREZ_001.columns = ['магазин', 'значение', 'дата']
            data = data[1:]
            data.columns = ['магазин', 'значение', 'дата']
            # объединение данных
            FINREZ = pd.concat([FINREZ_001, data]).reset_index(drop=True)
            FINREZ = RENAME().Rread(name_data=FINREZ, name_col="магазин")
            FLOAT().float_colm(name_data=FINREZ, name_col="значение")
            FINREZ = FINREZ.drop(index=FINREZ.index[-12:])
            print(FINREZ.tail(20))
            FINREZ.to_csv(PUT + "Финрез\\Прогнозный\\↓Финрез_прогнозный_прибыль без роялти.csv", encoding="ANSI", sep=';',
                         index=False, decimal=',')

Finrez().Finrez_prognozniy()