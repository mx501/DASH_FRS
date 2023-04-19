import psutil
import shutil
import xlsxwriter
from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time
from pandas.tseries.offsets import MonthBegin
import os
import pandas as pd
from tqdm.auto import tqdm
import sys
import math
import gc
import requests
# from memory_profiler import profile
import numpy as np
import calendar
#import bot_TELEGRAM as bot
import winsound
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

geo = "h"
# region расположение данных home или work

if geo == "h":
    # основной каталог расположение данных дашборда
    PUT = "D:\\Python\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
    """Путь до не разбитых файлов"""
    PUT_SEBES = "D:\\Python\\DASHBRD_SET\\Источники\\Себестоемость\\Исходные\\"
    """Путь до не разбитых файлов"""
    PUT_SEBES_day = "D:\\Python\\DASHBRD_SET\\Источники\\Себестоемость\\Архив\\"
    """Путь до источника"""
    PUT_SET = "C:\\Users\\виталий\\Desktop\\Паблик\\"
    """путь переноса файла"""
    PUT_SET_copy = "D:\\Python\\DASHBRD_SET\\Источники\\Чеки_сет\\Текущий день\\"
    """сохранение файла продаж"""
    PUT_SET_sales = "D:\\Python\\DASHBRD_SET\\Продаж_Set\\"
    """сохранение файла чеков"""
    PUT_SET_chek = "D:\\Python\\DASHBRD_SET\\ЧЕКИ_set\\"
else:
    # основной каталог расположение данных дашборда
    PUT = "C:\\Users\\lebedevvv\\Desktop\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текущий год\\"
    """ """
    PUT_CHEK = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\ЧЕКИ\\2023\\"
    """ Чеки паблик """
    PUT_SET = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Чеки_\\Исходники\\"
    """Чеки скопированные в свою папку для дадьнейшей обработки"""
    PUT_SET_COPY = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Чеки_\\Обработанные\\"

    PUT_SET_to = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Чеки_\\Обработанные\\"
# endregion
# region  Переборка всех файлов сета или последние
OBNOVLENIE = 1
OBNOVLENIE_file_all = "y"
# endregion


class MEMORY:
    def mem(self, x, text):
        total_memory_usage = x.memory_usage(deep=True).sum()
        print(text + " - Использовано памяти: {:.2f} MB".format(total_memory_usage / 1e6))
    """использование памяти датафрейм"""
    def mem_total(self,x):
        process = psutil.Process()
        memory_info = process.memory_info()
        total_memory_usage = memory_info.rss
        print(x +" - Использование памяти: {:.2f} MB".format(total_memory_usage / 1024 / 1024))
    """использование памяти программой полная"""
"""счетчик памяти"""
class RENAME:
    def Rread(self, name_data, name_col, name):
        replacements = pd.read_excel(PUT + "DATA_2\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")
        rng = len(replacements)
        for i in tqdm(range(rng), desc="Переименование - " + name, colour="#808080"): name_data[name_col] = \
            name_data[name_col].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
        return name_data
    """функция переименование"""
    def TY(self):
        # загрузка файла справочника териториалов
        ty = pd.read_excel("https://docs.google.com/spreadsheets/d/1rwsBEeK_dLdpJOAXanwtspRF21Z3kWDvruani53JpRY/export?exportFormat=xlsx")

        ty = ty[["Название 1 С (для фин реза)", "Менеджер"]]
        RENAME().Rread(name_data = ty, name_col= "Название 1 С (для фин реза)", name="TY")
        ty = ty.rename(columns={"Название 1 С (для фин реза)": 'магазин'})

        return ty
    """Справочник Территориальных управляющих"""
"""Отвечает за переименование и подгрузку справочнкиов готовых"""
class DOC:
    def to_(self, x, name, pyt):
        x.to_csv(pyt, encoding="ANSI", sep=';',
                 index=False, decimal=',')
    def to(self, x, name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal='.')
        return x

    def to_POWER_BI(self, x, name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal=',')

    def to_ERROR(self, x, name):
        x.to_csv(PUT + "ERROR\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal=',')
    def to_TEMP_txt(self, x, name):
        x.to_csv(PUT + "TEMP\\" + name, encoding="utf-8", sep='\t',
                 index=False, decimal='.')

    def to_TEMP(self, x, name):
        x.to_csv(PUT + "TEMP\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal='.')
    def to_exel(self, x, name):
        x.to_excel(PUT + "TEMP\\" + name, index=False)
"""функция сохранения файлов по папкам"""
class OPEN:
    def Day_fales(self):
        for root, dirs, files in os.walk(PUT_SEBES):
            for file in files:
                file_path = os.path.join(root, file)

                df = pd.read_csv(file_path, sep="\t", encoding="utf-8", skiprows=2,
                                 names=("По дням", "Склад магазин.Наименование", "Номенклатура", "Себестоимость", "ВесПродаж"))
                df = df.loc[df["Склад магазин.Наименование"] != "Итого"]
                df = df.loc[df["По дням"] != "Итого"]
                l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
                for w in l_mag:
                    df = df[~df['Склад магазин.Наименование'].str.contains(w)]
                # Получите уникальные даты из столбца "По дням"
                dates = df["По дням"].unique()
                # Переберите каждую дату
                for date in dates:
                    day_df = df[df["По дням"] == date]
                    file_name = os.path.join(PUT_SEBES_day, date + ".txt")
                    day_df.to_csv(file_name, sep="\t", encoding="ANSI", decimal=".", index=False)
    """разбиение файлов на дни себестоемость"""
    def open_exel(self, put):
        # получение списка файлов в указанном пути
        all_files = []
        for root, dirs, files in os.walk(put):
            for file in files:
                all_files.append(os.path.join(root, file))
        return all_files
    """отвечает за поик папок в указанном пути EXEL"""
    def open_CSV(self):
        return
    """отвечает за поик папок в указанном пути CSV"""
    def open_posledniy(self, put, number):
        print(put)
        poisk_2max = os.listdir(put)
        format = '%d.%m.%Y'
        fail = [f for f in poisk_2max if f.endswith('.xlsx') and len(f) > 10 and datetime.strptime(f[:10], format)]
        fail.sort(key=lambda x: datetime.strptime(x[:10], format))
        latest_files = fail[-number:]

        # Копируем последние 2 файла в папку x
        for file in latest_files:
            source_file = os.path.join(put, file)
            destination_file = os.path.join(PUT_SET_copy, file)
            shutil.copy(source_file, destination_file)

        # Возвращаем пути к скопированным файлам
        files = [os.path.join(PUT_SET_copy, f) for f in latest_files]
        print(files)
        return files
"""тветчает поиск фалов в папках"""
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
    def float_colm(self, name_data, name_col , name):
        print("Форматирование столбцов в формат FLOAT: " + name + ": " + name_col  )
        name_data[name_col ] = (name_data[name_col ].astype(str)
                                          .str.replace("\xa0", "")
                                          .str.replace(",", ".")
                                          .fillna("0")
                                          .astype("float")
                                          .round(2))
        return name_data
    """для одного столбца"""
"""тветчает за присвоение чсловых значени"""
class SET_RETEIL:
    def C_1(self):
        OPEN().Day_fales()

        return
    """отвечает за загрузкуданных сибестоймости из 1 с"""
    def Set_sales(self):
        SET_RETEIL().Set_chek()
        files = OPEN().open_posledniy(put=PUT_SET, number= 1)
        for file in files:
            MEMORY().mem_total(x="Загрузка - Set_sales: " + os.path.basename(file))
            set_sales_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            # фильтрация таблицы продаж
            set_sales_01 = set_sales_01.loc[set_sales_01["Тип"] == "Продажа"]
            set_sales_01 = set_sales_01.drop("Тип", axis=1)
            # даление подарочных карт
            PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                       "подарочная карта КМ 500 НОВАЯ",
                       "подарочная карта КМ 1000 НОВАЯ")
            for x in PODAROK:
                set_sales_01 = set_sales_01[~set_sales_01['Наименование товара'].str.contains(x)]
            RENAME().Rread(name_data=set_sales_01, name_col="Магазин 1C", name="Set_sales")
            set_sales = set_sales_01[["Дата/Время чека", "Магазин","Магазин 1C", "Код товара", "Наименование товара", "Стоимость позиции", "Количество", "Сумма скидки"]]
            del set_sales_01
            gc.collect()
            # Убрать часы
            set_sales["Дата/Время чека"] = set_sales["Дата/Время чека"].dt.date
            # задание формата для столбцов
            li_set_sales = ("Стоимость позиции","Количество", "Сумма скидки")
            FLOAT().float_colms(name_data= set_sales, name_col=li_set_sales , name="set_sales")
            # Групировки по дням
            set_sales = set_sales.groupby(["Магазин 1C", "Дата/Время чека", "Магазин","Код товара", "Наименование товара"], as_index=False).agg({
                                            "Стоимость позиции": "sum",
                                            "Сумма скидки": "sum",
                                            "Количество": "sum"}).reset_index(drop=True)

            DOC().to_(x=set_sales, pyt= PUT_SET_sales + os.path.basename(file)[:-5]+ ".csv", name=os.path.basename(file))
            MEMORY().mem_total(x="Обработан - Set_sales: " + os.path.basename(file))
            del set_sales
            gc.collect()
    """твечает за загрузку данных о продажах етретейла"""
    def Set_chek(self):
        files = OPEN().open_posledniy(put=PUT_SET, number= 1)
        MEMORY().mem_total(x="Загрузка - Set_chek: ")
        for file in files:
            set_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            # фильтрация таблицы продаж
            set_01 =set_01.loc[set_01["Тип"] == "Продажа"]
            set_01 = set_01.drop("Тип", axis=1)
            # даление подарочных карт
            PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                       "подарочная карта КМ 500 НОВАЯ",
                       "подарочная карта КМ 1000 НОВАЯ")
            for x in PODAROK:
                set_01 = set_01[~set_01['Наименование товара'].str.contains(x)]
            # таблица для обработки
            set_check = set_01[["Магазин 1C","Магазин","Дата/Время чека","Касса","Чек", "Стоимость позиции","Код товара"]]
            del set_01
            gc.collect()
            MEMORY().mem_total(x="Закгрузка файла чеков")

            # замена названий в файлах магазины
            RENAME().Rread(name_data=set_check, name_col="Магазин 1C", name="set_check")

            set_check["Дата/Время чека"] = set_check["Дата/Время чека"].dt.date
            # Формирование ID Чека
            set_check["ID_Chek"] =  set_check["Магазин"].astype(int).astype(str) + set_check["Касса"].astype(int).astype(str) + set_check["Чек"].astype(int).astype(str) + set_check["Дата/Время чека"].astype(str)
            set_check = set_check.drop(["Касса","Чек"], axis=1)

            # удаление не нужных символов
            FLOAT().float_colm(name_data=set_check, name_col="Стоимость позиции", name="set_check")
            # Групировки по дням
            set_check = set_check.groupby(["Магазин 1C", "Магазин", "Дата/Время чека", "ID_Chek"], as_index=False).agg({
                "Стоимость позиции": "sum",
                "Код товара": [("Количество товаров в чеке", "count"), ("Количество уникальных товаров в чеке", "nunique")]})
            # переименовываем столбцы
            set_check.columns = ['Магазин 1C', "Магазин", 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']
            # выбираем нужные столбцы и сортируем по дате/времени чека в порядке убывания
            set_check = set_check[["Магазин", 'Магазин 1C', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']] \
                .sort_values('Дата/Время чека', ascending=False) \
                .reset_index(drop=True)
            # групировка по магазинам
            set_check = set_check.groupby(["Магазин","Магазин 1C", "Дата/Время чека"], as_index=False) \
                .agg({"Стоимость позиции": "sum",
                      'ID_Chek':"count",
                      "Количество товаров в чеке": "mean",
                      "Количество уникальных товаров в чеке": "mean"}) \
                .sort_values("Дата/Время чека", ascending=False).reset_index(drop=True)
            # дбавление среднего чека
            set_check["Средний чек"] = set_check["Стоимость позиции"] / set_check["ID_Chek"]
            # переименование столбцов
            set_check = set_check.rename(columns={"Магазин": "ID_магазина"})
            set_check = set_check.rename(columns={"Магазин 1C": "магазин","Дата/Время чека":"дата","Стоимость позиции":"выручка",
                                                  "ID_Chek": "ID_чека","Количество товаров в чеке": "количество товаров в чеке","Количество уникальных товаров в чеке":"количество уникальных товаров в чеке" })

            # округление
            set_check = set_check.round(2)
            set_check['дата'] = pd.to_datetime(set_check['дата'], format='%Y-%m-%d')
            DOC().to_(x=set_check, pyt= PUT_SET_chek + os.path.basename(file)[:-5]+ ".csv", name=os.path.basename(file))
            del set_check
            gc.collect()
            MEMORY().mem_total(x="Обработан - set_check: " + os.path.basename(file))
"""Обработка данных сетретейла сомвмещение с себестоймостью из 1 с"""
class PRONOZ:
    def Prognoz_sales(self):
        return
    """Прогноз продаж"""
    def Prognoz_chek(self):
        return
    """Прогноз чеки"""
"""ПРогноз продаж"""
class SPRAVKA:
    def Magazin(self):
        return
    """Отвечает за формирование справочника"""
"""ормирование справочников"""


#SET_RETEIL().Set_sales()

OPEN().Day_fales()