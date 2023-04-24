
# Поиск сходства
from Levenshtein import distance
import distance
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
import bot_TELEGRAM as bot
import winsound
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

geo = "h"
# region расположение данных home или work

if geo == "h":
    # основной каталог расположение данных дашборда
    PUT = "D:\\Python\\DASHBRD_SET\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
    """Путь до не разбитых файлов"""
    PUT_SEBES = "D:\\Python\\DASHBRD_SET\\Источники\\Себестоемость\\Исходные\\"
    """Путь до разбитых файлов по дням"""
    PUT_SEBES_day = "D:\\Python\\DASHBRD_SET\\Источники\\Себестоемость\\Архив\\"
    """Путь до источника"""
    PUT_SET = "D:\\Python\\DASHBRD_SET\\Источники\\паблик\\"
    """путь переноса файла"""
    PUT_SET_copy = "D:\\Python\\DASHBRD_SET\\Источники\\Чеки_сет\\Текущий день\\"
    """сохранение файла продаж"""
    PUT_SET_sales = "D:\\Python\\DASHBRD_SET\\Продаж_Set\\Текущий день\\"
    """сохранение файла чеков"""
    PUT_SET_chek = "D:\\Python\\DASHBRD_SET\\ЧЕКИ_set\\Текущий день\\"
else:
    PUT = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
    """Путь до не разбитых файлов"""
    PUT_SEBES = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Источники\\Себестоемость\\Исходные\\"
    """Путь до разбитых файлов по дням"""
    PUT_SEBES_day = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Источники\\Себестоемость\\Архив\\"
    """Путь до источника"""
    PUT_SET = "P:\\Фирменная розница\\ФРС\\Данные из 1 С\\Чеки Set\\"
    """путь переноса файла"""
    PUT_SET_copy = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Источники\\Чеки_сет\\Текущий день\\"
    """сохранение файла продаж"""
    PUT_SET_sales = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Продаж_Set\\Текущий день\\"
    """сохранение файла чеков"""
    PUT_SET_chek = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\ЧЕКИ_set\\Текущий день\\"
# endregion
# region Переборка всех файлов сета или последние
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
        print("Загрузка справочника магазинов...")
        replacements = pd.read_excel("https://docs.google.com/spreadsheets/d/1SfuC2zKUFt6PQOYhB8EEivRjy4Dz-o4WDL-IR7CT3Eg/export?exportFormat=xlsx")
        """replacements = pd.read_excel(PUT + "Справочники\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")"""
        rng = len(replacements)
        for i in tqdm(range(rng), desc="Переименование - " + name, colour="#808080"): name_data[name_col] = \
            name_data[name_col].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
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
    def Nomenklatura_set(self):
        # Пути к несовпавшим наименованиям
        all_files_error = OPEN().open_pyt_fail(put=PUT + "\\Ошибки\\Себестоймость\\")
        # Пути к файлам с файлами данных сетретейла
        all_files_set_unic = OPEN().open_pyt_fail(put=PUT + "\\Ошибки\\Номенклатура сет\\")

        """обработка не совпавших названий сетретейл с 1 с"""
        Nomenklatura_error = pd.DataFrame()
        for file in  all_files_error:
            # Загружаем данные из файла и добавляем в DataFrame
            data = pd.read_csv(file, sep="\t", encoding="ANSI",skiprows=1, names=["номенклатура"])
            print(data)
            Nomenklatura_error = pd.concat([Nomenklatura_error, data], axis=0)
            del data
            gc.collect()
        # уникальные значения номенклатуры
        Nomenklatura_error = Nomenklatura_error["номенклатура"].unique()
        # уникальные значения номенклатуры итоговый датафрем
        Nomenklatura_error = pd.DataFrame(Nomenklatura_error, columns=["номенклатура"])
        Nomenklatura_error_itog = Nomenklatura_error
        # получаем список уникальных значений из колонки "номенклатура" в датафрейме Nomenklatura_error
        nomenklatura_error_list = Nomenklatura_error["номенклатура"].unique().tolist()
        DOC().to_(x=Nomenklatura_error, pyt=PUT + "Ошибки\\2.csv", name="dwd")

        """Уникальные значения номенклатуры сетретейла"""
        Nomenklatura_set = pd.DataFrame()
        for file in  all_files_set_unic:
            # Загружаем данные из файла и добавляем в DataFrame
            data = pd.read_csv(file, sep="\t", encoding="ANSI", skiprows=1, names=["номенклатура"])
            Nomenklatura_set = pd.concat([Nomenklatura_set, data], axis=0)
            del data
            gc.collect()
        # уникальные значения номенклатуры
        Nomenklatura_set = Nomenklatura_set["номенклатура"].unique()
        # уникальные значения номенклатуры итоговый датафрем
        Nomenklatura_set = pd.DataFrame(Nomenklatura_set, columns=["номенклатура"])
        # уникальные значения номенклатуры
        DOC().to_(x=Nomenklatura_set, pyt=PUT + "Ошибки\\1.csv", name="dwd")

        # функция для поиска наиболее похожего значения
        def find_similar(row):
            max_similarity = 0
            similar_value = ''
            for value in Nomenklatura_set["номенклатура"]:
                sim = 1 - (distance(value, row["номенклатура"]) / max(len(value), len(row["номенклатура"])))
                if sim > max_similarity:
                    max_similarity = sim
                    similar_value = value
            return similar_value

        # создаем новый столбец в df1 со значениями из df2

        Nomenklatura_error_itog['похожая_номенклатура'] =  Nomenklatura_error.apply(find_similar, axis=1)

        #Nomenklatura_error = Nomenklatura_error[Nomenklatura_error["номенклатура"].isin(nomenklatura_error_list)]
        #Nomenklatura_error = Nomenklatura_error.loc[Nomenklatura_error["номенклатура"].isin(nomenklatura_error_list)]

        #Nomenklatura_error = Nomenklatura_error.merge(Zakup[["магазин", "дата", 'ставка закуп ндс']],
                      #  on=["магазин", "дата"], how="left")

        print(Nomenklatura_error)



        DOC().to_(x=Nomenklatura_error, pyt=PUT + "Ошибки\\3.csv", name="dwd")
        return
"""Отвечает за переименование и подгрузку справочнкиов готовых"""
class DOC:
    def to_(self, x, name, pyt):
        x.to_csv(pyt, encoding="ANSI", sep=';',
                 index=False, decimal=',')
    def TO_EXEL(self, x, name, pyt):
        x.to_excel(pyt,index=False)

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
                os.path.basename(file)
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-8", skiprows=2,
                                 names=("По дням", "Склад магазин.Наименование", "Номенклатура", "Себестоимость", "ВесПродаж"))
                df = df.loc[df["Склад магазин.Наименование"] != "Итого"]
                df = df.loc[df["По дням"] != "Итого"]
                l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
                for w in l_mag:
                    df = df[~df['Склад магазин.Наименование'].str.contains(w)]
                dates = df["По дням"].unique()
                for date in dates:
                    day_df = df[df["По дням"] == date]
                    file_name = os.path.join(PUT_SEBES_day, date + ".txt")
                    day_df.to_csv(file_name, sep="\t", encoding="utf-8", decimal=".", index=False)
                    MEMORY().mem_total(x="Разбиение по дням: " + os.path.basename(file))
                del df
    """разбиение файлов на дни себестоемость"""
    def open_pyt_fail(self, put):
        # получение списка файлов в указанном пути
        all_files = []
        for root, dirs, files in os.walk(put):
            for file in files:
                all_files.append(os.path.join(root, file))
        return all_files
    """отвечает за поик папок в указанном пути CSV"""
    def open_posledniy(self, put, number):
        # выбираю последние файлы в папке по названию
        poisk_2max = os.listdir(put)
        format = '%d.%m.%Y'
        fail = [f for f in poisk_2max if f.endswith('.xlsx') and len(f) > 10 and datetime.strptime(f[:10], format)]
        fail.sort(key=lambda x: datetime.strptime(x[:10], format))
        latest_files = fail[-number:]
        # копирую в свою папку
        for file in latest_files:
            source_file = os.path.join(put, file)
            destination_file = os.path.join(PUT_SET_copy, file)
            shutil.copy(source_file, destination_file)
        # пути из своей папки
        files = [os.path.join(PUT_SET_copy, f) for f in latest_files]
        print(files)
        return files
    def Sebes_put(self,pyt , name):
        if os.path.exists(pyt + name[:-5] + ".txt"):
            sebes = pd.read_csv(pyt + name[:-5] + ".txt", skiprows=1, sep="\t", encoding="utf-8", parse_dates=["дата"], dayfirst=True,
                                    names=("дата", "магазин", 'номенклатура_1с', "cебестоимость", "вес_продаж"))
            RENAME().Rread(name_data=sebes, name_col="магазин", name="sebes")

            ln = ("cебестоимость","вес_продаж")
            FLOAT().float_colms(name_data=sebes, name_col=ln, name="sebes")

            #sebes = sebes.loc[(sebes["cебестоимость"].notnull()) | (sebes["вес_продаж"].notnull())]
            PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                       "подарочная карта КМ 500 НОВАЯ",
                       "подарочная карта КМ 1000 НОВАЯ")
            for x in PODAROK:
                sebes = sebes[~sebes['номенклатура_1с'].str.contains(x)]
            sebes_sum_do = sebes["cебестоимость"].sum()
            l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
            for w in l_mag:
                sebes = sebes[~sebes["магазин"].str.contains(w)]
        else:
            sebes = pd.DataFrame(columns=["дата", "магазин", 'номенклатура_1с', "cебестоимость", "вес_продаж"])
            sebes_sum_do = 0

        return sebes, sebes_sum_do
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
        # сибестоймость обработка
        return
    """отвечает за загрузкуданных сибестоймости из 1 с"""
    def Set_sales(self):
        files = OPEN().open_posledniy(put=PUT_SET, number= 40)
        #SET_RETEIL().Set_chek()
        Error_shtrix = pd.DataFrame()
        for file in files:
            MEMORY().mem_total(x="Загрузка - Set_sales: " + os.path.basename(file))
            set_sales_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            # фильтрация таблицы продаж
            set_sales_01 = set_sales_01.loc[set_sales_01["Тип"].notnull()]
            set_sales_01 = set_sales_01.drop("Тип", axis=1)
            # удаление подарочных карт
            PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                       "подарочная карта КМ 500 НОВАЯ",
                       "подарочная карта КМ 1000 НОВАЯ")
            for x in PODAROK:
                set_sales_01 = set_sales_01[~set_sales_01['Наименование товара'].str.contains(x)]


            RENAME().Rread(name_data=set_sales_01, name_col="Магазин 1C", name="Set_sales")
            set_sales = set_sales_01[["Дата/Время чека", "Магазин","Магазин 1C", "Код товара", "Наименование товара", "Штрихкод", "Стоимость позиции", "Количество", "Сумма скидки"]]
            del set_sales_01
            gc.collect()
            # Убрать часы
            set_sales["Дата/Время чека"] = set_sales["Дата/Время чека"].dt.date
            # задание формата для столбцов
            li_set_sales = ("Стоимость позиции","Количество", "Сумма скидки")
            FLOAT().float_colms(name_data= set_sales, name_col=li_set_sales , name="set_sales")
            # Групировки по дням
            set_sales = set_sales.groupby(["Магазин 1C", "Дата/Время чека", "Магазин","Код товара", "Наименование товара","Штрихкод"], as_index=False).agg({
                                            "Стоимость позиции": "sum",
                                            "Сумма скидки": "sum",
                                            "Количество": "sum"}).reset_index(drop=True)
            # переименование столбцов
            set_sales = set_sales.rename(columns={"Магазин 1C": "магазин","Магазин":"id_магазин","Код товара":"id_номенклатура","Штрихкод":"штрихкод","Дата/Время чека": "дата","Наименование товара": "номенклатура","Стоимость позиции": "выручка",
                                                  "Сумма скидки": "скидка", "Количество": "количество"})
            l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
            for w in l_mag:
                set_sales = set_sales[~set_sales["магазин"].str.contains(w)]
            # задать формат даты
            set_sales["дата"] = pd.to_datetime(set_sales["дата"], format="%Y-%m-%d")
            # Загузка названий с 1 с
            spravka_nom = pd.read_csv(PUT + "\\Справочники\\Справочник номенклатуры\\1.txt", sep="\t",skiprows=1, encoding="utf-8",
                                      names=( 'номенклатура_1с',"cрок_годности","группа", "подгруппа",  "штрихкод", ))
            spravka_dop = pd.read_excel(PUT + "\\Справочники\\Справочник номенклатуры\\Коректировка штрих кодов.xlsx")
            #spravka_nom = pd.concat([spravka_nom,spravka_dop ],axis=0)

            set_sales[ "штрихкод"] = set_sales[ "штрихкод"].astype("str").str.replace(".0", "")
            spravka_nom["штрихкод"] = spravka_nom["штрихкод"].astype("str").str.replace(".0", "")
            spravka_nom[ "штрихкод_1c"] =spravka_nom["штрихкод"]
            set_sales["штрихкод_set"] = set_sales[ "штрихкод"]
            set_sales = set_sales.merge(spravka_nom[['номенклатура_1с', "штрихкод", "штрихкод_1c"]],
                                on=["штрихкод"], how="left").reset_index(drop=True)
            set_rename = set_sales[["номенклатура","штрихкод_set","номенклатура_1с","штрихкод_1c" ]]
            set_rename = set_rename.drop_duplicates().reset_index(drop=True)

            rng_ = len(set_rename)
            print(rng_)
            for i in tqdm(range(rng_), desc="Переименование - номенклатуры", colour="#808080"):
                print(set_rename["номенклатура"][i])
                set_sales["номенклатура"] = \
                set_sales["номенклатура"].replace(set_rename["номенклатура"][i], set_rename["номенклатура_1с"][i], regex=False)


            # загрузка себистоемости
            sebes, sebes_sum_do = OPEN().Sebes_put(pyt  = PUT_SEBES_day, name =os.path.basename(file))
            sebes = sebes.loc[(sebes["cебестоимость"].notnull()) | (sebes["вес_продаж"].notnull()  )]

            #sebes = sebes.merge(spravka_nom[['номенклатура_1с', "штрихкод_1c"]],
                                        #on=['номенклатура_1с'], how="left").reset_index(drop=True)

            #DOC().TO_EXEL(x=sebes, pyt=PUT_SET_sales + os.path.basename(file)[:-5] + '545454fff.xlsx', name=os.path.basename(file))




            set_sales = set_sales.merge(sebes[["магазин", "дата","cебестоимость", "вес_продаж",'номенклатура_1с']],
                on=["магазин", "дата", 'номенклатура_1с',], how="outer").reset_index(drop=True)

            set_sales = set_sales[["магазин","дата","id_магазин",'номенклатура_1с',"выручка","скидка","количество","cебестоимость","вес_продаж"]]
            set_sales = set_sales.drop_duplicates(["магазин",'номенклатура_1с',"дата", "cебестоимость","вес_продаж"])
            # region ОБРАБОТКА ОТСУТСТВИЯ ШТРИХКОДА
            sebes_sum_posle = set_sales["cебестоимость"].sum().round(2)
            sebes_raz  = sebes_sum_posle- sebes_sum_do

            # подсчет количествоа без штрихкода сохранение в фаил
            Error_shtrix_01 = set_sales.copy()
            Error_shtrix_01 = Error_shtrix_01.loc[Error_shtrix_01["id_магазин"].isnull()]
            ln_not_sctrix = len(Error_shtrix_01['номенклатура_1с'].unique())
            Error_shtrix = pd.concat([Error_shtrix, Error_shtrix_01], axis=0)

            del Error_shtrix_01
            gc.collect()

            bot.BOT().bot_mes(mes=str(os.path.basename(file)[:-5]) +
                                  "\nДо: " + str(sebes_sum_do.round(2)) +
                                  "\nПосле: " + str(sebes_sum_posle.round(2)) +
                                  "\nРазница: " + str(sebes_raz.round(2))+
                                  "\nНет штрихкода: " + str(ln_not_sctrix))
            # endregion


            DOC().TO_EXEL(x=set_sales, pyt= PUT_SET_sales + os.path.basename(file)[:-5]+ '.xlsx', name=os.path.basename(file))
            MEMORY().mem_total(x="Обработан - Set_sales: " + os.path.basename(file))
            del set_sales
            gc.collect()
        DOC().to_(x=Error_shtrix, pyt=PUT + "Ошибки\\НЕТ_штрихкода.csv", name="dwd")
    """твечает за загрузку данных о продажах етретейла"""
    def Set_chek(self):
        files = OPEN().open_posledniy(put=PUT_SET, number= 20)
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
            set_check_date = set_check["Дата/Время чека"].max()
            with open(PUT + "Дата и время обновления\DATE.txt", "w") as f:
                f.write(str(set_check_date))
            del set_check_date
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
    def Nomenckaltura(self):
        nom = pd.read_csv(PUT + "Справочники\\Справочник номенклатуры\\1.txt", sep="\t",encoding="utf-8", skiprows= 1, names=("дата","Наименование","Группа(альт)","Подгруппа(альт)"))
        nom = nom.drop("дата", axis=1)
        nom= nom.drop_duplicates()
        nom_group_prod = pd.read_excel(PUT + "Справочники\\Справочник номенклатуры\\Справочник Калькулятор франшизы.xlsx")

        nom_group_prod = nom_group_prod.rename(columns={"Номенклатурная группа": "Группа(альт)","Группа товаров":"Группа товаров Франшиза"})
        nom = nom.merge(nom_group_prod, on=["Группа(альт)"], how="left")
        nom = nom.fillna("0")
        nom.to_excel(PUT+"Справочники\\Справочник номенклатуры\\С.xlsx", index=False)
        nom_group_prodakt = pd.read_excel("https://docs.google.com/spreadsheets/d/1dNt8qpZL_ST8aF_iBqV7oVQvH1tsExMd6uLCiC_UtfQ/export?exportFormat=xlsx")
        nom_group_prodakt = nom_group_prodakt.rename(columns={"Входит в группу": "Группа(альт)"})

        nom = nom.merge(nom_group_prodakt, on=["Группа(альт)"], how="left")
        nom = nom.fillna("0")
        nom.to_excel(PUT + "Справочники\\Справочник номенклатуры\\k.xlsx", index=False)
        print(nom_group_prodakt)
        print(nom)

        return
    def Nomenckaltura_obrabotka(self):
        files = OPEN().open_posledniy(put=PUT_SET, number=30)
        set_sales = pd.DataFrame()
        for file in files:
            MEMORY().mem_total(x="Загрузка - Set_sales: " + os.path.basename(file))
            set_sales_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            # фильтрация таблицы продаж
            set_sales_01 = set_sales_01.loc[set_sales_01["Тип"] == "Продажа"]
            set_sales_01 = set_sales_01.drop("Тип", axis=1)
            set_sales_01 = set_sales_01.rename(
                columns={"Магазин 1C": "магазин", "Магазин": "id_магазин", "Код товара": "id_номенклатура", "Штрихкод": "штрихкод", "Дата/Время чека": "дата", "Наименование товара": "номенклатура",
                         "Стоимость позиции": "выручка",
                         "Сумма скидки": "скидка", "Количество": "количество"})
            #RENAME().Rread(name_data=set_sales_01, name_col="Магазин 1C", name="Set_sales")
            set_sales_01 = set_sales_01[["id_номенклатура",  "номенклатура", "штрихкод"]]
            set_sales_01 = set_sales_01.drop_duplicates()
            set_sales = pd.concat([set_sales, set_sales_01], axis=0)

            del set_sales_01
            gc.collect()
        spravka_nom = pd.read_csv(PUT + "\\Справочники\\Справочник номенклатуры\\1.txt", skiprows=1, sep="\t", encoding="utf-8",
                                  names=('номенклатура-1с', "cрок_годности", "группа", "подгруппа", "штрихкод"))

        spravka_nom["штрихкод"] = spravka_nom["штрихкод"].astype("str")
        set_sales["штрихкод"] = set_sales["штрихкод"].astype("str").str.replace(".0", "")

        set_sales = set_sales.merge(spravka_nom[["номенклатура-1с", "штрихкод", "cрок_годности", "группа", "подгруппа"]],
                            on=["штрихкод"], how="left").reset_index(drop=True)
        set_sales = set_sales.drop_duplicates()

        set_sales.to_csv(PUT + "Ошибки\\4.csv", encoding="utf-8", sep=';',
                         index=False)



        return
    """Отвечает за формирование справочника"""
"""ормирование справочников"""

#OPEN().Day_fales()
"""соеденение продаж сетретейла с себестоемостью"""
SET_RETEIL().Set_sales()
#OPEN().Sebes_put()
"""Обработка справочника номенклатуры"""
#SPRAVKA().Nomenckaltura_obrabotka()
#RENAME().Nomenklatura_set()


