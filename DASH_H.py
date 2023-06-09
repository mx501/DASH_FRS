import psutil
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

# расположение данных home или work
geo = "h"
if geo == "h":
    # основной каталог расположение данных дашборда
    PUT = "D:\\Python\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
    PUT_SET = "D:\\Python\\Dashboard\\Чеки_\\Исходники\\"
    PUT_SET_to = "D:\\Python\\Dashboard\\Чеки_\\Обработанные\\"
else:
    # основной каталог расположение данных дашборда
    PUT = "C:\\Users\\lebedevvv\\Desktop\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текущий год\\"
    PUT_CHEK = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\ЧЕКИ\\2023\\"
    PUT_SET = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Чеки_\\Исходники\\"
    PUT_SET_to = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Чеки_\\Обработанные\\"
# region ОБНОВЛЕНИЕ ИСТОРИИ
HISTORY = "n"
CHECK_SET_ALL = "n"
# endregion



class MEMORY:
    def mem(self, x, text):
        total_memory_usage = x.memory_usage(deep=True).sum()
        print(text + " - Использовано памяти: {:.2f} MB".format(total_memory_usage / 1e6))
    def mem_total(self,x):
        process = psutil.Process()
        memory_info = process.memory_info()
        total_memory_usage = memory_info.rss
        print(x +" - Использование памяти: {:.2f} MB".format(total_memory_usage / 1024 / 1024))

class RENAME:
    def Rread(self):
        replacements = pd.read_excel(PUT + "DATA_2\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")
        rng = len(replacements)
        return rng, replacements
    '''блок переименования'''
    def HOZY(self):
        Spisania_HOZI = pd.read_csv(PUT + "хозы справочник\\1.txt", sep="\t", encoding='utf-8', skiprows=8,
                                    names=("магазин", "Номенклатура", "Сумма", "Сумма без НДС"))
        Spisania_HOZI = Spisania_HOZI["Номенклатура"].unique()
        return Spisania_HOZI
    '''блок хозы'''
"""чтение файлов для замены назани магазинов и базы номенклатуры хоз оваров"""
class DOC:
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
"""Автоописание"""
class NEW:
    # ##### Перезапись истории #######################
    def History(self):
        print("обновление истории")
        rng, replacements = RENAME().Rread()
        path = "D:\\Python\\Dashboard\\Исходные данные\\"  ##главный каталог
        path_to = "D:\\Python\\Dashboard\\ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\"   ##Куда
        papki_1uroven = os.listdir(path)  ##  смотрит папки главный каталог
        for god in papki_1uroven:
            os.mkdir(path_to + god)  ##  создает  каталоги для года
            papki_2uroven = os.listdir(path + god)  ##  смотрит папки внутри года каталог
            for mon in papki_2uroven:  ##  выбирает месяц
                os.mkdir(path_to + god + "\\" + mon)  ##  создает  каталоги для выбранного месяца
                files = os.listdir(path + god + "\\" + mon)  ## смотрит файлы в выбраннном месяце
                for f in tqdm(files, desc="ГОДА   --  ", ncols=130):  ##  переберает файлы
                    file = path + god + "\\" + mon + "\\" + f
                    stroka = open(file, 'r', encoding='utf-8')
                    new_name = f
                    new_name = new_name[0:23]  ##  берет имя для нового файла
                    stroka.close()  ##  закрывает файл
                    df = pd.read_csv(file, sep="\t", encoding='utf-8', parse_dates=['По дням'],dayfirst=True,)
                    # удаление лишних столбцов
                    df = df.drop(["СписРуб"], axis=1)
                    if 'Количество списания' in df.columns:
                       df = df.drop('Количество списания', axis=1)
                    if 'Списания, кг' in df.columns:
                        df = df.drop('Списания, кг', axis=1)
                    # переименовние магазинв
                    for i in tqdm(range(rng), desc="Переименование тт Продажи - ", colour="#808080"): df[
                        'Склад магазин.Наименование'] = \
                        df['Склад магазин.Наименование'].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                    # Обработка файла списания
                    spis = os.listdir(PUT+ "Списания\\"  + god + "\\" + mon)
                    for s in spis:
                        # открывает аналогичный года по маке файла продаж
                        file_s = PUT+ "Списания\\" + god + "\\" + mon + "\\" + s
                        spisisania = pd.read_csv(file_s, sep="\t", encoding='utf-8', skiprows=7, parse_dates=['По дням'],dayfirst=True,
                                              names=("Склад магазин.Наименование", "Номенклатура", 'По дням', "операции списания","СписРуб","списруб_без_ндс" ))
                        # переименование магазинов
                        for i in tqdm(range(rng), desc="Переименование тт Списания - ", colour="#808080"): spisisania[
                            'Склад магазин.Наименование'] = \
                            spisisania['Склад магазин.Наименование'].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                        # Фильтрация файла списания меньше или равно файлам продаж дтаа
                        max_sales = df['По дням'].max()
                        min_sales = df['По дням'].min()
                        spisisania = spisisania.loc[(spisisania['По дням'] <= max_sales) & (spisisania['По дням'] >= min_sales)]
                        # убрать строку итого
                        spisisania = spisisania.loc[spisisania["Склад магазин.Наименование"] != "Итого"]
                        # чистка мусора продажи
                        df["Выручка"] = df["Выручка"].str.replace(',', '.')
                        df["Выручка"] = df["Выручка"].str.replace('\xa0', '')
                        df["Выручка"] = df["Выручка"].astype("float")
                        # так как столбец списаний удален то убрать пустые строки
                        df = df.loc[df["Выручка"] > 0]
                        # чистка мусора списания
                        spisisania["СписРуб"] = spisisania["СписРуб"].str.replace(',', '.')
                        spisisania["СписРуб"] = spisisania["СписРуб"].str.replace('\xa0', '')
                        spisisania["СписРуб"] = spisisania["СписРуб"].astype("float")
                        spisisania = spisisania.loc[spisisania["СписРуб"] > 0]
                        # Сообщешие в телеграм
                        spisisania_do = spisisania["СписРуб"].copy()
                        df_do = df["Выручка"].copy()

                        # обьеденение таблиц списания и продаж
                        df = pd.concat([df, spisisania], axis=0)

                        # Сообщешие в телеграм
                        spisisania_ps = spisisania["СписРуб"].copy()
                        df_ps = df["Выручка"]
                        # Сообщешие в телеграм
                        print(new_name +"\nВыручка:" + str(df_ps.sum() - df_do.sum())+ "\nСписания:" + str(spisisania_ps.sum() - spisisania_do.sum()))
                        #BOT().bot_mes(mes= new_name +"\nВыручка:" + str(df_ps.sum() - df_do.sum())+ "\nСписания:" + str(spisisania_ps.sum() - spisisania_do.sum()))

                    df.to_csv(path_to + god + "\\" + mon + "\\" + new_name + ".txt", encoding='utf-8', decimal=",", sep="\t",
                              index=False)  ##  сохраняет файл
                    duration = 1000
                    freq = 220
                    winsound.Beep(freq, duration)
                    # очистка памяти
                    spisisania = pd.DataFrame()
                    df = pd.DataFrame()
                    gc.enable()
                    print(new_name, " готов")
                print(mon, ' готов')  ##  Заканчивает месяц, папку в годе, потом бере след.
            print(god, "готов")  ##  Заканчивает год , берет следующую
        print("ГОТОВО")
        return
    """Обновление истории (~2 час)"""
    def Check_set_all(self):
        gc.collect()
        MEMORY().mem_total(x="Функция Check_set")
        all_files = []
        for root, dirs, files in os.walk(PUT_SET):
            for file in files:
                all_files.append(os.path.join(root, file))
        # Список таблиц с данными за текущий месяц
        for file in all_files:
            set_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            set_check = set_01[["Тип","Магазин 1C","Магазин","Дата/Время чека","Касса","Чек", "Стоимость позиции","Код товара"]]
            del set_01
            gc.collect()
            MEMORY().mem_total(x="Закгрузка файла чеков")
            # фильтрация таблицы продаж
            set_check = set_check.loc[set_check["Тип"] == "Продажа"]
            set_check = set_check.drop("Тип", axis=1)
            # замена названий в файлах магазины
            rng, replacements = RENAME().Rread()
            for i in tqdm(range(rng), desc="Переименование тт Продажи - ", colour="#808080"): set_check[
                "Магазин 1C"] = \
                set_check["Магазин 1C"].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
            del rng, replacements
            set_check["Дата/Время чека"] = set_check["Дата/Время чека"].dt.date
            # Формирование ID Чека
            set_check["ID_Chek"] =  set_check["Магазин"].astype(int).astype(str) + set_check["Касса"].astype(int).astype(str) + set_check["Чек"].astype(int).astype(str) + set_check["Дата/Время чека"].astype(str)
            set_check = set_check.drop(["Магазин","Касса","Чек"], axis=1)
            # удаление не нужных символов
            set_check["Стоимость позиции"] = (set_check["Стоимость позиции"].astype(str)
                           .str.replace("\xa0", "")
                           .str.replace(",", ".")
                           .fillna("0")
                           .astype("float")
                           .round(2))

            # Групировки по дням
            set_check = set_check.groupby(["Магазин 1C", "Дата/Время чека", "ID_Chek"], as_index=False).agg({
                "Стоимость позиции": "sum",
                "Код товара": [("Количество товаров в чеке", "count"), ("Количество уникальных товаров в чеке", "nunique")]})
            # переименовываем столбцы
            set_check.columns = ['Магазин 1C', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']
            # выбираем нужные столбцы и сортируем по дате/времени чека в порядке убывания
            set_check = set_check[['Магазин 1C', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']] \
                .sort_values('Дата/Время чека', ascending=False) \
                .reset_index(drop=True)
            # групировка по магазинам
            set_check = set_check.groupby(["Магазин 1C", "Дата/Время чека"], as_index=False) \
                .agg({"Стоимость позиции": "sum",
                      'ID_Chek':"count",
                      "Количество товаров в чеке": "mean",
                      "Количество уникальных товаров в чеке": "mean"}) \
                .sort_values("Дата/Время чека", ascending=False).reset_index(drop=True)
            # дбавление среднего чека
            set_check["Средний чек"] = set_check["Стоимость позиции"] / set_check["ID_Chek"]
            # переименование столбцов
            set_check = set_check.rename(columns={"Магазин 1C": "Магазин","Дата/Время чека":"Дата","Стоимость позиции":"Выручка",
                                                  "ID_Chek": "Чеков","Количество товаров в чеке": "Длина","Количество уникальных товаров в чеке":"SKU в чеке" })
            # округление
            set_check = set_check.round(2)
            set_check['Дата'] = pd.to_datetime(set_check['Дата'], format='%Y-%m-%d')
            print(set_check)
            set_check['Дата'] = set_check['Дата'].dt.strftime('%d.%m.%Y')
            print(set_check)
            set_check.to_excel(PUT_SET_to + os.path.basename(file), index=False)
            del set_check
            gc.collect()
            MEMORY().mem_total(x="Завершение цикла обработки чеков сета")
    """ Обработка чеков сетретейла папка полностью"""
    # #################################################
    def Check_set(self):
        gc.collect()
        MEMORY().mem_total(x="Функция Check_set")
        poisk_2max = os.listdir(PUT_SET)
        format = '%d.%m.%Y'
        fail = [f for f in poisk_2max if f.endswith('.xlsx') and len(f) > 10 and datetime.strptime(f[:10], format)]

        fail.sort(key=lambda x: datetime.strptime(x[:10], format))
        latest_files = fail[-2:]

        file_paths = [os.path.join(PUT_SET, f) for f in latest_files]
        print(file_paths)
        # Список таблиц с данными за текущий месяц
        for file in file_paths:
            set_01 = pd.read_excel(file, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y %H:%M:%S" )
            set_check = set_01[["Тип","Магазин 1C","Магазин","Дата/Время чека","Касса","Чек", "Стоимость позиции","Код товара"]]
            del set_01
            gc.collect()
            MEMORY().mem_total(x="Закгрузка файла чеков")
            # фильтрация таблицы продаж
            set_check = set_check.loc[set_check["Тип"] == "Продажа"]
            set_check = set_check.drop("Тип", axis=1)
            # замена названий в файлах магазины
            rng, replacements = RENAME().Rread()
            for i in tqdm(range(rng), desc="Переименование тт Продажи - ", colour="#808080"): set_check[
                "Магазин 1C"] = \
                set_check["Магазин 1C"].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
            del rng, replacements
            set_check["Дата/Время чека"] = set_check["Дата/Время чека"].dt.date
            # Формирование ID Чека
            set_check["ID_Chek"] =  set_check["Магазин"].astype(int).astype(str) + set_check["Касса"].astype(int).astype(str) + set_check["Чек"].astype(int).astype(str) + set_check["Дата/Время чека"].astype(str)
            set_check = set_check.drop(["Магазин","Касса","Чек"], axis=1)
            # удаление не нужных символов
            set_check["Стоимость позиции"] = (set_check["Стоимость позиции"].astype(str)
                           .str.replace("\xa0", "")
                           .str.replace(",", ".")
                           .fillna("0")
                           .astype("float")
                           .round(2))

            # Групировки по дням
            set_check = set_check.groupby(["Магазин 1C", "Дата/Время чека", "ID_Chek"], as_index=False).agg({
                "Стоимость позиции": "sum",
                "Код товара": [("Количество товаров в чеке", "count"), ("Количество уникальных товаров в чеке", "nunique")]})
            # переименовываем столбцы
            set_check.columns = ['Магазин 1C', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']
            # выбираем нужные столбцы и сортируем по дате/времени чека в порядке убывания
            set_check = set_check[['Магазин 1C', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']] \
                .sort_values('Дата/Время чека', ascending=False) \
                .reset_index(drop=True)
            # групировка по магазинам
            set_check = set_check.groupby(["Магазин 1C", "Дата/Время чека"], as_index=False) \
                .agg({"Стоимость позиции": "sum",
                      'ID_Chek':"count",
                      "Количество товаров в чеке": "mean",
                      "Количество уникальных товаров в чеке": "mean"}) \
                .sort_values("Дата/Время чека", ascending=False).reset_index(drop=True)
            # дбавление среднего чека
            set_check["Средний чек"] = set_check["Стоимость позиции"] / set_check["ID_Chek"]
            # переименование столбцов
            set_check = set_check.rename(columns={"Магазин 1C": "Магазин","Дата/Время чека":"Дата","Стоимость позиции":"Выручка",
                                                  "ID_Chek": "Чеков","Количество товаров в чеке": "Длина","Количество уникальных товаров в чеке":"SKU в чеке" })
            # округление
            set_check = set_check.round(2)
            set_check['Дата'] = pd.to_datetime(set_check['Дата'], format='%Y-%m-%d')
            print(set_check)
            set_check['Дата'] = set_check['Дата'].dt.strftime('%d.%m.%Y')
            print(set_check)
            set_check.to_excel(PUT_SET_to + os.path.basename(file), index=False)
            del set_check
            gc.collect()
            MEMORY().mem_total(x="Завершение цикла обработки чеков сета")
    """ Обработка чеков сетретейла только последние 2 файла"""
    def STATYA(self):
        STATYA = pd.read_excel(PUT + "DATA_2\\" + "@СПРАВОЧНИК_СТАТЕЙ.xlsx",
                               sheet_name="STATYA_REDAKT")
        return STATYA
    '''справочник статей_редактируется в ексель'''
    def Dat_nalog_kanal(self):
        Dat_canal_nalg = pd.read_csv(PUT + "TEMP\\" + "Дата_канал_налог.csv",
                                     sep=";", encoding='ANSI', parse_dates=['дата'])
        # вычисление максимального месыяца
        finrez_max_month = Dat_canal_nalg[["дата"]]
        finrez_max_month = finrez_max_month.reset_index(drop=True)
        finrez_max_month = finrez_max_month.loc[finrez_max_month['дата'] >= "2023-01-01"]
        finrez_max_month = finrez_max_month.reset_index(drop=True)
        finrez_max_month['месяц'] = finrez_max_month['дата'].dt.month
        finrez_max_month = finrez_max_month['месяц'].max()
        # вычисление максимальной даты в формате гггг-мм-дд
        finrez_max_data = Dat_canal_nalg[["дата"]]
        finrez_max_data = finrez_max_data.reset_index(drop=True)
        finrez_max_data = finrez_max_data.loc[finrez_max_data['дата'] >= "2023-01-01"]
        finrez_max_data = finrez_max_data.reset_index(drop=True)
        finrez_max_data['дата'] = finrez_max_data['дата'].dt.date
        finrez_max_data = finrez_max_data['дата'].max()
        finrez_max_data = pd.to_datetime(finrez_max_data)
        print("Вычисление максимальных дат финреза")
        return Dat_canal_nalg, finrez_max_month, finrez_max_data
    '''отвечает за загрузку данных каналов и режима налога, используется для вычисления максимальной и минимальной даты и месяца'''
    def Finrez(self):
        rng, replacements = RENAME().Rread()
        print(
            "Обновление финреза\n")
        for files in os.listdir(PUT + "DATA\\"):
            FINREZ = pd.read_excel(PUT + "DATA\\" + files, sheet_name="Динамика ТТ исходник")
            FINREZ = FINREZ.rename(columns={"Торговая точка": "магазин", "Дата": "дата",
                                            "Канал": "канал",
                                            "Режим налогообложения": "режим налогообложения",
                                            "Канал на последний закрытый период": "канал на последний закрытый период"})
            print("файл - ", files)
            for i in tqdm(range(rng), desc="Переименование магазинов   --  ", ncols=120, colour="#F8C9CE"):
                FINREZ["магазин"] = FINREZ["магазин"].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                              regex=False)
            FINREZ = FINREZ.reset_index(drop=True)
            FINREZ = FINREZ.loc[FINREZ['дата'] >= "2022-01-01"]

            # region для получения уникальных значений колонок
            FINREZ_SPRAVOCHNIK_STATIYA = FINREZ.melt(
                id_vars=["дата", "магазин", "режим налогообложения", "канал", "канал на последний закрытый период"],
                var_name="статья",
                value_name="значение")
            unique_values = FINREZ_SPRAVOCHNIK_STATIYA["статья"].unique()
            FINREZ_SPRAVOCHNIK_STATIYA = pd.DataFrame({'статья': unique_values})
            DOC().to_exel(x=FINREZ_SPRAVOCHNIK_STATIYA, name="Справоник статей.xlsx")
            del unique_values
            del FINREZ_SPRAVOCHNIK_STATIYA
            gc.collect()
            # endregion
            # region выбор столбцов в файле
            FINREZ = FINREZ[
                ["дата", "магазин", "режим налогообложения", "канал", "канал на последний закрытый период",
                 "Товарооборот (продажи) МКП, ед", "Товарооборот (продажи) МКП, руб с НДС",
                 "Товарооборот (продажи) КП, ед",
                 "Товарооборот (продажи) КП, руб с НДС", "Товарооборот (продажи) сопутка, ед",
                 "Товарооборот (продажи) сопутка, руб с НДС",
                 # ---Доход
                 "Выручка Итого, руб без НДС",
                 "Прочие доходы (субаренда), руб без НДС", "Прочие доходы (утилизация), руб без НДС",
                 "Доход от продажи ТМЦ, руб без НДС",
                 "Прочие доходы (паушальный взнос, услуги по открытию), руб без НДС", "Доход Штрафы, руб без НДС",
                 "Доход Аренда помещений, руб без НДС",
                 "Доход (аренда оборудования), руб без НДС", "Доход Роялти, руб без НДС",
                 "Доход комиссионное вознаграждение, руб без НДС",
                 "Доход Услуги по договору комиссии интернет-магазин, руб без НДС",
                 # ---Закуп
                 "* Закуп товара (МКП, КП, сопутка), руб без НДС",
                 # ---Затраты
                 "ОЕ - Общие Операционные расходы (сумма всех статей расходов), руб без НДС",
                 "2.1. ФОТ+Отчисления", "2.2. Аренда", "2.19. Бонусы программы лояльности",
                 "2.3.1. Электроэнергия", "2.3.2. Вывоз мусора, ЖБО, ТБО",
                 "2.3.3. Тепловая энергия",
                 "2.3.4. Водоснабжение",
                 "2.3.5. Водоотведение",
                 "2.3.6. Прочие коммунальные услуги (ФРС)",
                 "2.3.7. Газоснабжение",
                 "2.11. Маркетинговые расходы",
                 "2.9. Налоги",
                 "2.5.2. НЕУ",
                 "2.10. Питание сотрудников ",
                 "2.17. Распределяемая аналитика",
                 "2.18. Затраты службы развития",
                 "2.3.8. Охрана",
                 "2.4. Услуги банка",
                 "2.7. Прочие прямые затраты",
                 "2.7.1. Дезинфекционные средства",
                 "2.7.10. Услуги сотовой связи",
                 "2.7.2. Канцелярские товары",
                 "2.7.3. Командировочные расходы",
                 "2.7.4. Медицинские услуги, медикаменты, медосмотры",
                 "2.7.5. Расходы на аренду прочего имущества",
                 "2.7.6. Спецодежда, спецобувь, СИЗ",
                 "2.7.7. Транспортные услуги",
                 "2.7.8. Интернет",
                 "2.7.9. Услуги по дератизации, дезинсекции",
                 "2.16. Роялти",
                 "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)",
                 "2.13. Инструменты/инвентарь",
                 "2.14. Ремонт и содержание зданий, оборудования",
                 "2.15.ТО оборудования (аутсорсинг)",
                 "2.6. Хозяйственные товары",
                 "2.8. ТМЦ ",
                 "Рентабельность, %",
                 "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС",
                 "Наценка Общая, руб без НДС",
                 "Наценка Общая, %",
                 "Наценка МКП и КП, руб с НДС",
                 "Наценка сопутка, руб с НДС",
                 "Наценка МКП и КП, %",
                 "Наценка сопутка, %",
                 ##
                 "Доля колбаса",
                 "Доля п/ф",
                 "Доля  гриль",
                 "Доля  Кости ливер отруба",
                 "Доля куриные п/ф",
                 "Доля субпродукты кур",
                 "Доля сопутка",
                 "Доля Калина малина",
                 "Доля зеленый магазин",
                 "Доля Волков Кофе",
                 "Доля \"Изготовлено по заказу\"",
                 "Доля Рыбные п/ф",
                 "Доля Продукция кулинарного цеха КХВ",
                 "Доля Пекарня",
                 # Инвестиции
                 "Инвестиции 3.1. Маркетинговые расходы",
                 "Инвестиции 3.2. Инструменты/инвентарь",
                 "Инвестиции 3.3. Ремонт и содержание зданий, оборудования",
                 "3.3.1. Инвестиции на переформат и открытие",
                 "3.3.2. Инвестиции на переформат и открытие Оборудование (тех служба ФРС)",
                 "3.3.3. Инвестиции на переформат и открытие Ремонт (тех служба ФРС)",
                 "Инвестиции 3.4. ТО оборудования (аутсорсинг)",
                 # точка безубыточности
                 "Точка безубыточности (МКП, КП, Сопутка), руб с НДС",
                 "Разница между точкой безубыточности и объемом продаж, руб с НДС",
                 "Среднесписочная численность персонала на ТТ",
                 "Средняя з/пл с отчислениями",
                 ###
                 "1.1.Закуп товара (МКП и КП), руб с НДС",
                 "1.2.Закуп товара (сопутка), руб с НДС",
                 "Выручка Итого, руб с НДС"]]
            # endregion
            # region получение числа коналов для каждого магазина для фильтрации ФРС, даты перехода магазина корректировка
            FINREZ_00 = FINREZ.groupby(["магазин", "дата"])['канал'].nunique().reset_index()
            FINREZ_00 = FINREZ_00.rename(columns={'канал': 'канал_кол'})
            FINREZ = pd.merge(FINREZ, FINREZ_00[['магазин', 'дата', 'канал_кол']], on=['магазин', 'дата'],
                              how='left')
            # даты пререхода на франшизу корректировка
            FINREZ.loc[(FINREZ['дата'] == '2022-07-01') & (FINREZ['магазин'] == 'Комсомольский, 34'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-08-01') & (FINREZ['магазин'] == 'Л-К, ул.Ленина, 50'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-07-01') & (FINREZ['магазин'] == 'Ленина, 133'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-07-01') & (FINREZ['магазин'] == 'Ленинградский, 30/1'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-05-01') & (FINREZ['магазин'] == 'Ленинградский, 45'), 'канал_кол'] = 1
            FINREZ.loc[
                (FINREZ['дата'] == '2022-06-01') & (FINREZ['магазин'] == 'Межд-к, пр.Шахтеров, 23А'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-02-01') & (FINREZ['магазин'] == 'Московский, 18'), 'канал_кол'] = 1
            FINREZ.loc[
                (FINREZ['дата'] == '2022-01-01') & (FINREZ['магазин'] == 'Новосиб, ул.Каменская, 44'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-05-01') & (FINREZ['магазин'] == 'Ноградская, 34'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-02-01') & (FINREZ['магазин'] == 'Октябрьский, 78'), 'канал_кол'] = 1
            FINREZ.loc[
                (FINREZ['дата'] == '2022-08-01') & (FINREZ['магазин'] == 'Осинники, Победы, 32'), 'канал_кол'] = 1
            FINREZ.loc[
                (FINREZ['дата'] == '2022-07-01') & (FINREZ['магазин'] == 'Полысаево, Космонавтов 82'), 'канал_кол'] = 1
            FINREZ.loc[
                (FINREZ['дата'] == '2022-07-01') & (FINREZ['магазин'] == 'Прокопьевск, Гагарина, 37'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-08-01') & (FINREZ['магазин'] == 'Терешковой, 22А'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-05-01') & (FINREZ['магазин'] == 'Шахтеров, 111'), 'канал_кол'] = 1
            FINREZ.loc[(FINREZ['дата'] == '2022-06-01') & (FINREZ['магазин'] == 'Шахтеров, 36'), 'канал_кол'] = 1

            # endregion
            # region вычисление доли
            r = ("Доля Калина малина", "Доля Пекарня", "Доля Продукция кулинарного цеха КХВ", "Доля Рыбные п/ф",
                 "Доля \"Изготовлено по заказу\"",
                 "Доля Волков Кофе", "Доля зеленый магазин", "Доля сопутка", "Доля субпродукты кур", "Доля куриные п/ф",
                 "Доля  Кости ливер отруба", "Доля  гриль", "Доля п/ф", "Доля колбаса")
            for Y in tqdm(r, desc="     Расчет", ncols=120, colour="#F8C9CE", ):
                FINREZ[Y] = FINREZ[Y] * FINREZ["Выручка Итого, руб с НДС"]

            # endregion
            # region наценки

            FINREZ["Закуп товара общий, руб с НДС"] = FINREZ["1.1.Закуп товара (МКП и КП), руб с НДС"] + FINREZ[
                "1.2.Закуп товара (сопутка), руб с НДС"]
            FINREZ.loc[FINREZ["режим налогообложения"] == "упрощенка", "Закуп(режм налога)"] = FINREZ[
                "Закуп товара общий, руб с НДС"]
            FINREZ.loc[FINREZ["режим налогообложения"] == "общий", "Закуп(режм налога)"] = FINREZ[
                "* Закуп товара (МКП, КП, сопутка), руб без НДС"]
            FINREZ.loc[FINREZ["канал"] == "Итого Франшиза", "Закуп(режм налога)"] = FINREZ["Наценка Общая, %"]
            FINREZ.loc[FINREZ["канал"] == "Итого ФРС", "Закуп(режм налога)"] = FINREZ["Наценка Общая, %"]
            FINREZ["Товарооборот КП + МКП, руб с НДС"] = FINREZ["Товарооборот (продажи) КП, руб с НДС"] + FINREZ[
                "Товарооборот (продажи) МКП, руб с НДС"]
            FINREZ["Товарооборот(Общий) с НДС"] = FINREZ["Товарооборот (продажи) КП, руб с НДС"] + FINREZ[
                "Товарооборот (продажи) МКП, руб с НДС"] + FINREZ["Товарооборот (продажи) сопутка, руб с НДС"]
            FINREZ["Наценка (Общий) с НДС"] = FINREZ["Наценка МКП и КП, руб с НДС"] + FINREZ[
                "Наценка сопутка, руб с НДС"]

            # endregion
            # переименование обобщения
            FINREZ.loc[FINREZ['магазин'] == "Офис", "канал"] = "Офис"
            FINREZ.loc[FINREZ['магазин'] == "Роялти ФРС", "канал"] = "Роялти ФРС"
            FINREZ = FINREZ.reset_index(drop=True)
            # сохранение временного файла с каналами и режимом налогобложения
            FINREZ_MAX = FINREZ[
                ["дата", 'магазин', 'режим налогообложения', 'канал', 'канал на последний закрытый период']]
            DOC().to_TEMP(x=FINREZ_MAX, name="Дата_канал_налог.csv")
            print("Сохранено - Дата_канал_налог.csv")
            del FINREZ_MAX
            gc.collect()

            # СПРАВОЧНИК РОЯЛТИ ЗА последние 3 месяца
            FINREZ_ROYALTY = FINREZ[["дата", "магазин", "Выручка Итого, руб без НДС", "Доход Роялти, руб без НДС"]]
            # Выбор строк, соответствующих последним трем месяцам, для каждого магазина
            aver3 = FINREZ_ROYALTY["дата"] >= (FINREZ_ROYALTY["дата"].max() - pd.DateOffset(months=2))
            FINREZ_ROYALTY= FINREZ_ROYALTY.loc[aver3]
            # Вычисление среднего значения выручки и роялти за последние три месяца для каждого магазина
            FINREZ_ROYALTY = FINREZ_ROYALTY.groupby('магазин')[[ "Выручка Итого, руб без НДС","Доход Роялти, руб без НДС"]].sum()
            # Округление значений до двух знаков после запятой
            FINREZ_ROYALTY = FINREZ_ROYALTY.round(2)
            # Сброс индекса и переименование столбцов
            FINREZ_ROYALTY.reset_index(inplace=True)
            FINREZ_ROYALTY["Роялти%"] = FINREZ_ROYALTY["Доход Роялти, руб без НДС"] / FINREZ_ROYALTY["Выручка Итого, руб без НДС"]
            FINREZ_ROYALTY["Роялти%"] = FINREZ_ROYALTY["Роялти%"].round(3)
            FINREZ_ROYALTY.loc[FINREZ_ROYALTY["Роялти%"] == 0,"Роялти%" ] = 0.041
            DOC().to_TEMP(x=FINREZ_ROYALTY, name="Роялти\\Роялти средние за 3 месяца.csv")
            del  FINREZ_ROYALTY
            gc.collect()

            # добавление закуп товара с НДС
            FINREZ["Закуп товара общий, руб с НДС"] = FINREZ["1.1.Закуп товара (МКП и КП), руб с НДС"] + \
                                                      FINREZ["1.2.Закуп товара (сопутка), руб с НДС"]
            FINREZ.loc[(FINREZ["канал"] == "ФРС") & (FINREZ["режим налогообложения"] == "упрощенка"),
            "* Закуп товара (МКП, КП, сопутка), руб без НДС"] = FINREZ["Закуп товара общий, руб с НДС"]

            # разворот таблицы фнреза
            FINREZ = FINREZ.melt(
                id_vars=["дата", "магазин", "режим налогообложения", "канал", "канал на последний закрытый период",
                         'канал_кол'],
                var_name="статья",
                value_name="значение")
            # очистка от мусора
            FINREZ['значение'] = FINREZ['значение'].astype("str")
            FINREZ['значение'] = FINREZ['значение'].str.replace(u'\xa0', "")
            FINREZ['значение'] = np.where((FINREZ['значение'] == 0), "nan", FINREZ['значение'])
            FINREZ['значение'] = np.where((FINREZ['значение'] == "-"), "nan", FINREZ['значение'])
            FINREZ['значение'] = np.where((FINREZ['значение'] == "#ДЕЛ/0!"), "nan", FINREZ['значение'])
            FINREZ['значение'] = np.where((FINREZ['значение'] == "#ЗНАЧ!"), "nan", FINREZ['значение'])
            FINREZ['значение'] = FINREZ['значение'].str.replace(",", ".")
            FINREZ = FINREZ.loc[(FINREZ['значение'] != "nan")]

            FINREZ['значение'] = FINREZ['значение'].astype("float")
            FINREZ = FINREZ.loc[(FINREZ['значение'] != 0)]
            # округление
            FINREZ['значение'] = FINREZ['значение'].round(2)
            # переименование названия закупа
            FINREZ.loc[FINREZ[
                           "статья"] == "* Закуп товара (МКП, КП, сопутка), руб без НДС", "статья"] = "Закуп товара (МКП, КП, сопутка), руб без НДС"
            # region добавление справочника сатей
            STATYA = NEW().STATYA()
            FINREZ = FINREZ.merge(STATYA[["статья", "фрс_расчет среднего",
                                          "фр_расчет чистой прибыли", "подгруппа", "группа",
                                          "фрс_расчет чистой прибыли", "удалить для фрс и аренда", "отбор"]],
                                  on=["статья"], how="left")
            # endregion

            # region убрать все значения для сочетания фрс где более 2х каналов в месяце
            FINREZ_Er = FINREZ.copy()
            mask = (FINREZ['канал'] == 'ФРС') & (FINREZ['канал_кол'] == 2) & (
                        FINREZ["удалить для фрс и аренда"] == 'да')
            FINREZ.loc[mask, 'значение'] = 0


            # добавление столбца для каскадных значений
            FINREZ["каскад"] = FINREZ["значение"]
            FINREZ.loc[FINREZ["группа"] == "Затраты, руб.(без НДС)", "каскад"] = -FINREZ["значение"]
            FINREZ.loc[FINREZ["группа"] == "Закуп, руб.(без НДС)", "каскад"] = -FINREZ["значение"]

            DOC().to_POWER_BI(x=FINREZ, name="Финрез_ФРСТЕСТ.csv")
            print(FINREZ)
            # деление таблиц на каналы
            # ################################################################# ФРС
            # ФРС только стать участвующие в чистой прибыли
            FINREZ_FRS = FINREZ.loc[FINREZ["канал"] == "ФРС"]
            FINREZ_FRS = FINREZ_FRS.loc[(FINREZ_FRS["фрс_расчет чистой прибыли"] == "да")]

            # добавление чистой прибыли
            grouped = FINREZ_FRS.groupby(
                ['магазин', 'дата', 'канал', "канал на последний закрытый период", "режим налогообложения"])

            sums = grouped['каскад'].agg('sum')
            new_row = pd.DataFrame({
                'магазин': sums.index.get_level_values('магазин'),
                'дата': sums.index.get_level_values('дата'),
                "канал на последний закрытый период": sums.index.get_level_values("канал на последний закрытый период"),
                "режим налогообложения": sums.index.get_level_values("режим налогообложения"),
                'канал': sums.index.get_level_values('канал'),
                "статья": 'чистая прибыль',
                'значение': sums.values,
                'каскад': sums.values})
            FINREZ_FRS = pd.concat([FINREZ_FRS, new_row], axis=0)
            # region ERROR ФРС
            FINREZ_Er = FINREZ_Er.loc[FINREZ_Er["канал"] == "ФРС"].copy()
            FINREZ_Er.loc[
                FINREZ_Er["статья"] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС", "статья"] = "чистая прибыль"
            FINREZ_ERROR = FINREZ_Er.loc[FINREZ_Er["статья"] == "чистая прибыль"].copy()
            FINREZ_ERROR = FINREZ_ERROR.rename(columns={"значение": "значение из итогов"})

            FINREZ_FRS_00 = FINREZ_FRS.copy()
            FINREZ_FRS_00 = FINREZ_FRS_00.loc[FINREZ_FRS_00["статья"] == "чистая прибыль"]
            FINREZ_ERROR_FRS = FINREZ_FRS_00.merge(
                FINREZ_ERROR[["дата", "значение из итогов", "магазин", "статья", 'канал']],
                on=["статья", "магазин", "дата", 'канал'], how="left")
            FINREZ_ERROR_FRS["расхождение"] = FINREZ_ERROR_FRS["значение"] - FINREZ_ERROR_FRS["значение из итогов"]
            FINREZ_ERROR_FRS = FINREZ_ERROR_FRS.loc[
                (FINREZ_ERROR_FRS["расхождение"] < -10) | (FINREZ_ERROR_FRS["расхождение"] > 10)]
            # endregion
            # добавление статей для фрс
            FINREZ_FRS_01 = FINREZ.loc[FINREZ["канал"] == "ФРС"]
            FINREZ_FRS_01 = FINREZ_FRS_01.loc[(FINREZ_FRS_01["отбор"] == "товароборот") |
                                              (FINREZ_FRS_01["отбор"] == "наценка") |
                                              (FINREZ_FRS_01["отбор"] == "доля") |
                                              (FINREZ_FRS_01["отбор"] == "инвестиции") |
                                              (FINREZ_FRS_01["отбор"] == "точка безубыточности") |
                                              (FINREZ_FRS_01["отбор"] == "персонал")]
            FINREZ_FRS = pd.concat([FINREZ_FRS, FINREZ_FRS_01], axis=0)
            # Фрс исключения для расчета рентабельности
            FINREZ_FRS.loc[(FINREZ_FRS["отбор"] == "товароборот") |
                           (FINREZ_FRS["отбор"] == "наценка") |
                           (FINREZ_FRS["отбор"] == "доля") |
                           (FINREZ_FRS["отбор"] == "инвестиции") |
                           (FINREZ_FRS["отбор"] == "точка безубыточности") |
                           (FINREZ_FRS["отбор"] == "персонал"), "каскад"] = 0

            FINREZ_FRS = FINREZ_FRS.reset_index(drop=True)

            # ################################################################# ФРАНШИЗА
            # ФРАНШИЗА только стать участвующие в чистой прибыли
            FINREZ_FRANSHIZA = FINREZ.loc[
                (FINREZ["канал"] == "Франшиза в аренду") | (FINREZ["канал"] == "Франшиза внешняя")]
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["фр_расчет чистой прибыли"] == "да")]

            # добавление чистой прибыли
            grouped = FINREZ_FRANSHIZA.groupby(
                ['магазин', 'дата', 'канал', "канал на последний закрытый период", "режим налогообложения"])
            sums = grouped['каскад'].agg('sum')
            new_row = pd.DataFrame({
                'магазин': sums.index.get_level_values('магазин'),
                'дата': sums.index.get_level_values('дата'),
                "канал на последний закрытый период": sums.index.get_level_values("канал на последний закрытый период"),
                "режим налогообложения": sums.index.get_level_values("режим налогообложения"),
                'канал': sums.index.get_level_values('канал'),
                "статья": 'чистая прибыль',
                'значение': sums.values,
                'каскад': sums.values})
            FINREZ_FRANSHIZA = pd.concat([FINREZ_FRANSHIZA, new_row], axis=0)
            # region ERROR ФР
            FINREZ_00 = FINREZ.copy()
            FINREZ_00.loc[
                FINREZ_00["статья"] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС", "статья"] = 'чистая прибыль'
            FINREZ_ERROR = FINREZ_00.loc[FINREZ_00["статья"] == 'чистая прибыль'].copy()
            FINREZ_ERROR = FINREZ_ERROR.rename(columns={"значение": "значение из итогов"})
            FINREZ_FRANSHIZA_00 = FINREZ_FRANSHIZA.copy()
            FINREZ_FRANSHIZA_00 = FINREZ_FRANSHIZA_00.loc[FINREZ_FRANSHIZA_00["статья"] == "чистая прибыль"]

            FINREZ_ERROR_FR = FINREZ_FRANSHIZA_00.merge(
                FINREZ_ERROR[["дата", "значение из итогов", "магазин", "статья", 'канал']],
                on=["статья", "магазин", "дата", 'канал'], how="left")
            FINREZ_ERROR_FR["расхождение"] = FINREZ_ERROR_FR["значение"] - FINREZ_ERROR_FR["значение из итогов"]
            FINREZ_ERROR_FR = FINREZ_ERROR_FR.loc[
                (FINREZ_ERROR_FR["расхождение"] < -10) | (FINREZ_ERROR_FR["расхождение"] > 10)]
            # endregion
            # добавление выручки без ндс для франшизы
            FINREZ_FRANSHIZA_01 = FINREZ.loc[
                (FINREZ["канал"] == "Франшиза в аренду") | (FINREZ["канал"] == "Франшиза внешняя")]
            FINREZ_FRANSHIZA_01 = FINREZ_FRANSHIZA_01.loc[
                (FINREZ_FRANSHIZA_01["статья"] == "Выручка Итого, руб без НДС")]

            FINREZ_FRANSHIZA_01.loc[FINREZ_FRANSHIZA_01[
                                        "статья"] == "Выручка Итого, руб без НДС", "статья"] = 'Выручка Итого, руб без НДС(для франшизы)'
            FINREZ_FRANSHIZA = pd.concat([FINREZ_FRANSHIZA, FINREZ_FRANSHIZA_01], axis=0)
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.reset_index(drop=True)

            # добавление Товарооборота без ндс для франшизы
            FINREZ_FRANSHIZA_01 = FINREZ.loc[
                (FINREZ["канал"] == "Франшиза в аренду") | (FINREZ["канал"] == "Франшиза внешняя")]
            FINREZ_FRANSHIZA_01 = FINREZ_FRANSHIZA_01.loc[(FINREZ_FRANSHIZA_01["отбор"] == "товароборот") |
                                                          (FINREZ_FRANSHIZA_01["отбор"] == "наценка") |
                                                          (FINREZ_FRANSHIZA_01["отбор"] == "доля") |
                                                          (FINREZ_FRS_01["отбор"] == "инвестиции")]
            FINREZ_FRANSHIZA = pd.concat([FINREZ_FRANSHIZA, FINREZ_FRANSHIZA_01], axis=0)
            # Фрс исключения для расчета рентабельности
            FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["отбор"] == "товароборот") |
                           (FINREZ_FRANSHIZA["отбор"] == "наценка") |
                           (FINREZ_FRANSHIZA["отбор"] == "доля") |
                           (FINREZ_FRANSHIZA["отбор"] == "инвестиции") |
                           (FINREZ_FRANSHIZA["отбор"] == "точка безубыточности") |
                           (FINREZ_FRANSHIZA["отбор"] == "персонал"), "каскад"] = 0
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.reset_index(drop=True)

            # ################################################################# ФРАНШИЗА
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.rename(columns={'значение': 'значение_фр', "каскад": "каскад_фр"})
            FINREZ_FRS = FINREZ_FRS.rename(columns={'значение': 'значение_фрс', "каскад": "каскад_фрс"})
            FINREZ = pd.concat([FINREZ_FRANSHIZA, FINREZ_FRS], axis=0)
            FINREZ = FINREZ.reset_index(drop=True)

            # сохранение временного файла для дальнецшей обработки
            DOC().to_ERROR(x=FINREZ_ERROR_FRS,
                           name="Ошики ФРС(сравнение чистой приыли из файла и вычесленой по статейно для каждого магазина.csv")
            DOC().to_ERROR(x=FINREZ_ERROR_FR,
                           name="Ошики франшиза(сравнение чистой приыли из файла и вычесленой по статейно для каждого магазина.csv")
            DOC().to_POWER_BI(x=FINREZ_FRANSHIZA, name="Финрез_Франшиза.csv")
            DOC().to_POWER_BI(x=FINREZ_FRS, name="Финрез_ФРС.csv")
            DOC().to_POWER_BI(x=FINREZ, name="Финрез_Обработанный.csv")
            print("Сохранено - Финрез_Обработанный.csv")
            return FINREZ
    '''обработка финреза итоговых значений'''
    def Obnovlenie(self):
        print("ОБНОВЛЕНИЕ ПРОДАЖ........\n")
        if HISTORY == "y" :
            NEW().History()
        rng, replacements = RENAME().Rread()
        l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
        for rootdir, dirs, files in os.walk(PUT + "NEW\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    df = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['По дням'], dayfirst=True, skiprows=3, names=(
                        ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                         'Себестоимость',
                         'Выручка', 'Прибыль']))
                    max_sales = df['По дням'].max().strftime('%Y-%m-%d')
                    min_sales = df['По дням'].min().strftime('%Y-%m-%d')
                    # чистка мусора продажи
                    df["Выручка"] = df["Выручка"].str.replace(',', '.')
                    df["Выручка"] = df["Выручка"].str.replace('\xa0', '')
                    df["Выручка"] = df["Выручка"].astype("float")
                    # так как столбец списаний удален то убрать пустые строки
                    df = df.loc[df["Выручка"] > 0]
                    for w in l_mag:
                        df = df[~df['Склад магазин.Наименование'].str.contains(w)]
                    # переименовние магазинв
                    for i in tqdm(range(rng), desc="Переименование тт Продажи - ", colour="#808080"): df[
                        'Склад магазин.Наименование'] = \
                        df['Склад магазин.Наименование'].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                    # Обработка файла списания
                    # открывает аналогичный года по маке файла продаж
                    file_s = PUT + "Списания\\Текущий месяц\\"
                    spisisania = None
                    for files in os.listdir(file_s):

                        spisisania = pd.read_csv(file_s+files, sep="\t", encoding='utf-8', skiprows=7, parse_dates=['По дням'], dayfirst=True,
                                                 names=("Склад магазин.Наименование", "Номенклатура", 'По дням', "операции списания", "СписРуб", "списруб_без_ндс"))
                        for w in l_mag:
                            spisisania = spisisania[~spisisania['Склад магазин.Наименование'].str.contains(w)]
                        # переименование магазинов
                        for i in tqdm(range(rng), desc="Переименование тт Списания - ", colour="#808080"): spisisania[
                            'Склад магазин.Наименование'] = \
                            spisisania['Склад магазин.Наименование'].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                        # Фильтрация файла списания меньше или равно файлам продаж дaта

                        spisisania = spisisania.loc[(spisisania['По дням'] <= max_sales) & (spisisania['По дням'] >= min_sales)]
                        # убрать строку итого
                        spisisania = spisisania.loc[spisisania["Склад магазин.Наименование"] != "Итого"]
                        # чистка мусора списания
                        spisisania["СписРуб"] = spisisania["СписРуб"].str.replace(',', '.')
                        spisisania["СписРуб"] = spisisania["СписРуб"].str.replace('\xa0', '')
                        spisisania["СписРуб"] = spisisania["СписРуб"].astype("float")
                        spisisania = spisisania.loc[spisisania["СписРуб"] > 0]
                        # Для сверки итоговых значений после слияния столбца результатты до слияния

                    # обьеденение таблиц списания и продаж
                    df = pd.concat([df, spisisania], axis=0)
                    # лог
                    # сохранение файла
                    MEMORY().mem(x=df, text="1")
                    df.to_csv(PUT_PROD + file, encoding='utf-8', sep="\t", decimal=",", index=False)
                    if geo == "w":
                        df.to_csv("P:\\Фирменная розница\\ФРС\\Данные из 1 С\\Продажи, Списания, Прибыль\\Текущий год\\" + file, encoding='utf-8', sep="\t", decimal=",", index=False)
                    MEMORY().mem(x=df, text="2")
                    # очистка памяти
                    del spisisania
                    del df
                NEW().Check_set()
                if ((file.split('.')[-1]) == 'xlsx'):
                    pyt_excel = os.path.join(rootdir, file)
                    read = pd.read_excel(pyt_excel, sheet_name="Sheet1")
                    for i in tqdm(range(rng), desc="Переименование тт чеки -" + file, ncols=120, colour="#F8C9CE", ):
                        read[
                            'Магазин'] = read['Магазин'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                                 regex=False)
                    read = read.reset_index(drop=True)
                    read.to_excel(PUT_CHEK + file,
                                  index=False)
                bot.BOT().bot_raschet()
                gc.collect()
        '''отвечает за загрузку и переименование новых данных продаж и чеков'''
    """Обновление данных ежедневное"""
    def NDS_vir(self):
        rng, replacements = RENAME().Rread()
        print("вычисление ставки ндс выручки\n")
        vir_NDS = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_выручка\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    vir_NDS_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8,
                                             names=("магазин", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in range(rng):
                        vir_NDS_00["магазин"] = vir_NDS_00["магазин"].replace(replacements["НАЙТИ"][i],
                                                                              replacements["ЗАМЕНИТЬ"][i], regex=False)
                    date = file[0:len(file) - 4]
                    vir_NDS_00 = vir_NDS_00.loc[vir_NDS_00["магазин"] != "Итого"]
                    vir_NDS_00["дата"] = date
                    vir_NDS_00["дата"] = pd.to_datetime(vir_NDS_00["дата"], dayfirst=True)
                    vir_NDS = pd.concat([vir_NDS, vir_NDS_00], axis=0)
                    del vir_NDS_00
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            vir_NDS[r] = vir_NDS[r].str.replace(',', '.')
            vir_NDS[r] = vir_NDS[r].str.replace('\xa0', '')
            vir_NDS[r] = vir_NDS[r].astype("float")
        vir_NDS["ставка выручка ндс"] = (vir_NDS["ПРОДАЖИ БЕЗ НДС"] / vir_NDS["ПРОДАЖИ С НДС"])
        vir_NDS["ПРОВЕРКАА"] = vir_NDS["ПРОДАЖИ С НДС"] * vir_NDS["ставка выручка ндс"]
        del rng, replacements, Ren
        return vir_NDS
    '''отвечает за загрузку данных для  расчета ставки выручки ндс'''
    def NDS_zakup(self):
        rng, replacements = RENAME().Rread()
        print("Расчет ставки ндс закуп\n")
        Zakup = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_закуп\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'csv'):
                    pyt_txt = os.path.join(rootdir, file)
                    Zakup_00 = pd.read_csv(pyt_txt, sep=";", encoding='ANSI', skiprows=1,
                                           names=("магазин", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС", 'ставка закуп ндс'))
                    for i in range(rng):
                        Zakup_00["магазин"] = Zakup_00["магазин"].replace(replacements["НАЙТИ"][i],
                                                                          replacements["ЗАМЕНИТЬ"][i],
                                                                          regex=False)
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].str.replace(',', '.')
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].str.replace(' ', '')
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].astype("float")
                    date = file[0:len(file) - 4]
                    Zakup_00 = Zakup_00.loc[Zakup_00["магазин"] != "Итого"]
                    Zakup_00["дата"] = date
                    Zakup_00["дата"] = pd.to_datetime(Zakup_00["дата"], dayfirst=True)
                    Zakup = pd.concat([Zakup, Zakup_00], axis=0)
                    gc.enable()
        return Zakup
    '''отвечает за загрузку данных для  расчета ставки питание с ндс'''
    def Stavka_nds_Kanal(self):
        Zakup = NEW().NDS_zakup()
        Dat_canal_nalg, finrez_max_month, finrez_max_data = NEW().Dat_nalog_kanal()
        sales = NEW().NDS_vir()
        print("формирование таблицы ставок ндс")

        # обьеденене ставок ндс
        NDS = sales.drop(['ПРОДАЖИ С НДС', 'ПРОДАЖИ БЕЗ НДС', 'ПРОВЕРКАА'], axis=1)
        NDS["хозы ставка ндс"] = 0.80
        NDS = NDS.merge(Zakup[["магазин", "дата", 'ставка закуп ндс']],
                        on=["магазин", "дата"], how="left")
        del Zakup
        # добавление режима налогобложения для установки ставки на упраенку 1'''
        canal_nalog_maxdate = Dat_canal_nalg["дата"].max()
        canal_nalog = Dat_canal_nalg.loc[Dat_canal_nalg['дата'] == canal_nalog_maxdate]
        NDS = NDS.merge(
            Dat_canal_nalg[["магазин", 'режим налогообложения', 'канал', 'канал на последний закрытый период']],
            on=["магазин"], how="outer")
        del Dat_canal_nalg, finrez_max_month, finrez_max_data
        NDS.loc[NDS['режим налогообложения'] == "упрощенка", ['ставка выручка ндс', "хозы ставка ндс",'ставка закуп ндс']] = [1, 1, 1]
        spisok_01 = ("Офис","Роялти ФРС","ФРС без затрат офиса","Франшиза без затрат офиса", "ФРС + Франшиза без затрат офиса","ИТОГО Розничная сеть")
        for i in spisok_01:
            NDS = NDS.loc[NDS["магазин"]!= i ]
        # тестовый
        DOC().to_TEMP(x=NDS, name="\\Ставки НДС\\НДС.csv")
        return NDS
    '''отвечает за обьеденение ставок nds  в одну таблицу вычисление налога для упращенки'''
    def Royalty(self):
        royalty = pd.read_csv(PUT + "TEMP\\Роялти\\Роялти средние за 3 месяца.csv", encoding="ANSI", sep=";", usecols=["магазин","Роялти%"] )
        return royalty
    """Вычисление ставки роялти"""
'''отвечает первоначальную обработку, сохранение временных файлов для вычисления минимальной и максимальной даты,
сохраненние вреенного файла с каналати и режимом налогобложения'''
class PROGNOZ:
    def SALES_obrabotka(self):
        MEMORY().mem_total(x="ОБНОВЛЕНИЕ СВОДНОЙ ПРОДАЖ\n")
        gc.collect()
        PROD_SVOD = pd.DataFrame()
        # загрузка данных финреза
        Dat_canal_nalg, finrez_max_month, finrez_max_data = NEW().Dat_nalog_kanal()
        # Поиск файлов текущих продаж. Список всех файлов в папке и подпапках
        all_files = []
        for root, dirs, files in os.walk(PUT_PROD):
            for file in files:
                all_files.append(os.path.join(root, file))
        # Загрузка файлов из списка
        PROD_SVOD = pd.DataFrame()
        for file in all_files:
            print("Фильтруется - " + os.path.basename(file))
            MEMORY().mem_total(x="")
            PROD_SVOD_00 = pd.read_csv(file, sep="\t", encoding='utf-8', parse_dates=['дата'], skiprows=1, low_memory=False,
                                       names=("магазин", "номенклатура", "дата", "количество_продаж",
                                              "вес_продаж", "Закуп товара общий, руб с НДС", "Выручка Итого, руб с НДС",
                                              "Наценка Общая, руб с НДС", "операции списания", "СписРуб", "списруб_без_ндс"))

            # создание столбца с месяцем для дальнейшей фильтрации
            PROD_SVOD_00['месяц_номер'] = PROD_SVOD_00['дата'].dt.month
            # Выбор дат из файлов которые больше чес дата финреза
            PROD_SVOD_00 = PROD_SVOD_00.loc[(PROD_SVOD_00['месяц_номер'] > finrez_max_month)]
            # присовение формата к столбцам float
            ln = ( "Выручка Итого, руб с НДС","СписРуб", "списруб_без_ндс", "Закуп товара общий, руб с НДС", "вес_продаж", "количество_продаж", "Наценка Общая, руб с НДС")
            for e in ln:
                PROD_SVOD_00[e] = (PROD_SVOD_00[e].astype(str)
                                .str.replace("\xa0", "")
                                .str.replace(",", ".")
                                .fillna("0")
                                .astype("float")
                                .round(2))
            # убрать из общей выручки подарочные карты
            PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                       "подарочная карта КМ 500 НОВАЯ",
                       "подарочная карта КМ 1000 НОВАЯ")
            for x in PODAROK:
                PROD_SVOD_00 = PROD_SVOD_00[~PROD_SVOD_00['номенклатура'].str.contains(x)]
            # удаление столбца с номенклатурой
            PROD_SVOD_00 = PROD_SVOD_00.drop(columns={"номенклатура"})
            PROD_SVOD_00["операции списания"] = PROD_SVOD_00["операции списания"].fillna('продажа')
            PROD_SVOD_00 = PROD_SVOD_00.groupby(["магазин", "операции списания", "дата"], as_index=False) \
                .agg({"количество_продаж": "sum",
                      "вес_продаж": "sum",
                      "Закуп товара общий, руб с НДС": "sum",
                      "Выручка Итого, руб с НДС": "sum",
                      "Наценка Общая, руб с НДС": "sum",
                      "СписРуб": "sum",
                      "списруб_без_ндс": "sum"}) \
                .sort_values("дата", ascending=False).reset_index()
            # удаление из списка магазинов не нужных магазинов
            l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
            for w in l_mag:
                PROD_SVOD_00 = PROD_SVOD_00[~PROD_SVOD_00["магазин"].str.contains(w)]

            # создание столбца где дата приводится к формату даты из финреза
            PROD_SVOD_00['месяц'] = pd.to_datetime(PROD_SVOD_00['дата'].dt.strftime('%Y-%m-01'))

            # обьеденение фильтрованых файлов
            PROD_SVOD = pd.concat([PROD_SVOD,PROD_SVOD_00], axis=0)
            PROD_SVOD = PROD_SVOD.reset_index(drop=True)
            # удаление промежуточной таблицы
            del PROD_SVOD_00
            gc.collect()
        # Вычисление количества отравботанных дней
        PROD_SVOD_01 = PROD_SVOD.groupby(["магазин", 'месяц'], as_index=False) \
            .agg({"дата": "nunique"})
        PROD_SVOD_01 = PROD_SVOD_01.rename(columns={"дата":"факт отработанных дней"})
        PROD_SVOD = PROD_SVOD.merge(PROD_SVOD_01, on=["магазин", "месяц"], how="left")

        # удаление промежуточной таблицы
        del PROD_SVOD_01,  Dat_canal_nalg, finrez_max_month, finrez_max_data
        gc.collect()

        # формирование таблицы по месяцам
        PROD_SVOD = PROD_SVOD.drop(columns={"дата"})
        PROD_SVOD = PROD_SVOD.rename(columns={"дата": "факт отработанных дней","месяц":"дата"})

        PROD_SVOD = PROD_SVOD.groupby(["магазин", "дата", "операции списания"], as_index=False) \
            .agg({"количество_продаж": "sum",
                  "вес_продаж": "sum",
                  "Закуп товара общий, руб с НДС": "sum",
                  "Выручка Итого, руб с НДС": "sum",
                  "Наценка Общая, руб с НДС": "sum",
                  "СписРуб": "sum",
                  "списруб_без_ндс": "sum",
                  "факт отработанных дней": "min"}) \
            .sort_values("магазин", ascending=False)
        MEMORY().mem_total(x="формирование таблицы по месяцам\n")

        # Создание столбцов аналогичне финрезу
        PROD_SVOD.loc[PROD_SVOD["операции списания"] ==  "Хозяйственные товары", "2.6. Хозяйственные товары" ] = PROD_SVOD["списруб_без_ндс"]
        PROD_SVOD.loc[PROD_SVOD["операции списания"] ==  "Питание сотрудников", "2.10. Питание сотрудников " ] = PROD_SVOD["списруб_без_ндс"]
        PROD_SVOD.loc[PROD_SVOD["операции списания"] == "МАРКЕТИНГ (блогеры, фотосессии)", "2.11. Маркетинговые расходы"] = PROD_SVOD["списруб_без_ндс"]
        # промежуточный столбец для расчета списания потерь
        PROD_SVOD.loc[(PROD_SVOD["операции списания"] == "ПОТЕРИ") |
                      (PROD_SVOD["операции списания"] == "Дегустации") |
                      (PROD_SVOD["операции списания"] == "Кражи") |
                      (PROD_SVOD["операции списания"] == "Подарок покупателю (сервисная фишка)") |
                      (PROD_SVOD["операции списания"] == "Подарок покупателю (бонусы)") , "РАСЧЕТ Списание потерь (до ноября 19г НЕУ + Списание потерь)"] = PROD_SVOD["списруб_без_ндс"]
        # вычисление столбца списани потерь
        PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] = np.nan
        # группировка
        grouped = PROD_SVOD.groupby(
            ['магазин', 'дата',])
        sums = grouped["РАСЧЕТ Списание потерь (до ноября 19г НЕУ + Списание потерь)"].agg('sum')
        new_row = pd.DataFrame({
            'магазин': sums.index.get_level_values('магазин'),
            'дата': sums.index.get_level_values('дата'),
            "операции списания": '"2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"',
            "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)": sums.values})
        PROD_SVOD = pd.concat([PROD_SVOD, new_row], axis=0)
        PROD_SVOD = PROD_SVOD.drop(columns={"РАСЧЕТ Списание потерь (до ноября 19г НЕУ + Списание потерь)"})

        # добавление ставки ндс вычисление выручки без ндс
        nds = NEW().Stavka_nds_Kanal()
        PROD_SVOD = PROD_SVOD.merge(nds, on=["магазин","дата"], how="left")
        PROD_SVOD = PROD_SVOD.drop_duplicates()
        PROD_SVOD['Выручка Итого, руб без НДС'] = PROD_SVOD['Выручка Итого, руб с НДС'] * PROD_SVOD["ставка выручка ндс"]
        PROD_SVOD = PROD_SVOD.drop(columns={"ставка выручка ндс"})
        PROD_SVOD['2.5.2. НЕУ'] = PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] * 0.15
        PROD_SVOD['2.9. Налоги'] = PROD_SVOD['Выручка Итого, руб с НДС'] * 0.01
        PROD_SVOD['2.4.Услуги банка'] = PROD_SVOD['Выручка Итого, руб с НДС'] * 0.0096
        PROD_SVOD["Закуп товара общий, руб без НДС"] = PROD_SVOD["Закуп товара общий, руб с НДС"] * PROD_SVOD['ставка закуп ндс']
        del grouped, sums, new_row, nds
        # добавление среднего роялти
        royalty = NEW().Royalty()
        PROD_SVOD = PROD_SVOD.merge(royalty, on=["магазин"], how="left")
        del royalty
        gc.collect()

        DOC().to_POWER_BI(x=PROD_SVOD, name="1.csv")
        MEMORY().mem_total(x="\n")
        return PROD_SVOD
    def Sales_prognoz(self):
        PROD_SVOD = pd.read_csv(PUT + "TEMP\\" + "Временный файл_продаж.csv",
                                sep=";", encoding='ANSI', parse_dates=['дата'])
        print("расчет прогноза продаж")
        # region ДОБАВЛЕНИЕ ДАННЫХ КАЛЕНДАРЯ
        Calendar = pd.read_excel(PUT + "DATA_2\\Календарь.xlsx", sheet_name="Query1")
        Calendar.loc[~Calendar["дата"].dt.is_month_start, "дата"] = Calendar["дата"] - MonthBegin()
        Calendar = Calendar.groupby(["ГОД", "НОМЕР МЕСЯЦА", "дата"], as_index=False) \
            .aggregate({'ДНЕЙ В МЕСЯЦЕ': "max"}) \
            .sort_values("ГОД", ascending=False)
        PROD_SVOD = PROD_SVOD.rename(columns={'Склад магазин.Наименование': "!МАГАЗИН!"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Месяц': 'дата'})
        PROD_SVOD = PROD_SVOD.merge(Calendar, on=["дата"], how="left")
        PROD_SVOD["Осталось дней продаж"] = PROD_SVOD["ДНЕЙ В МЕСЯЦЕ"] - PROD_SVOD["факт отработанных дней"]
        dd = PROD_SVOD.groupby('дата')['Осталось дней продаж'].aggregate('min')
        PROD_SVOD = PROD_SVOD.merge(dd, on=["дата"], how="left")
        PROD_SVOD.loc[
            PROD_SVOD["Осталось дней продаж_x"] > PROD_SVOD["Осталось дней продаж_y"], 'Осталось дней продаж_x'] = \
        PROD_SVOD["Осталось дней продаж_y"]
        PROD_SVOD = PROD_SVOD.drop(columns={"Осталось дней продаж_y", "НОМЕР МЕСЯЦА", "ГОД"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Осталось дней продаж_x': "Осталось дней продаж"})

        # region ДОБАВЛЕНИЕ КАНАЛОВ ОБОБЩАЮЩИХ В ТАБЛИЦУ ПРОДАЖ
        #canal = pd.read_excel(PUT + "DATA_2\\" + "Каналы.xlsx", sheet_name="Лист1")
        #canal["дата"] = canal["дата"].astype("datetime64[ns]")
        #PROD_SVOD = pd.concat([PROD_SVOD, canal], axis=0)
        #PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion
        # region РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ
        PROD_SVOD = PROD_SVOD.drop(columns={"ставка выручка ндс", "ставка списание без хозов ндс", "питание ставка ндс","хозы ставка ндс","ставка закуп ндс",})
        PROD_SVOD = PROD_SVOD.melt(
            id_vars=["дата", "магазин", "ДНЕЙ В МЕСЯЦЕ", "Осталось дней продаж", "факт отработанных дней","режим налогообложения","канал","канал на последний закрытый период"])
        PROD_SVOD = PROD_SVOD.rename(columns={"variable": "cтатья", "value": "значение"})
        # endregion
        PROD_SVOD["значение"] = PROD_SVOD["значение"].astype("float")
        PROD_SVOD["факт отработанных дней"] = PROD_SVOD["факт отработанных дней"].astype("float")
        # region добавление прогноза

        PROD_SVOD = PROD_SVOD.rename(columns={"значение": "значение_факт" })
        PROD_SVOD["значение"] = ((PROD_SVOD["значение_факт"] / PROD_SVOD["факт отработанных дней"]) * PROD_SVOD[
            "Осталось дней продаж"]) + PROD_SVOD["значение_факт"]
        PROD_SVOD[["значение","значение_факт"]] = PROD_SVOD[["значение","значение_факт"]].round(2)
        # endregion
        PROD_SVOD_00 = PROD_SVOD.groupby(["магазин", "дата"])['канал'].nunique().reset_index()
        PROD_SVOD_00 = PROD_SVOD_00.rename(columns={'канал': 'канал_кол', })
        PROD_SVOD = pd.merge(PROD_SVOD, PROD_SVOD_00[['магазин', 'дата', 'канал_кол']], on=['магазин', 'дата'], how='left')
        sp  = ["Выручка Итого, руб без НДС", "Закуп товара (МКП, КП, сопутка), руб без НДС", "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)", "2.5.2. НЕУ","2.6. Хозяйственные товары"]
        for i in sp:
            PROD_SVOD.loc[(PROD_SVOD["канал"] == "ФРС") & (
                    PROD_SVOD['канал_кол'] == 2) & (PROD_SVOD["cтатья"] == i), "значение" ] = 0
        #PROD_SVOD = PROD_SVOD.drop(columns={"ДНЕЙ В МЕСЯЦЕ"," канал_кол", "ГОД"})
        DOC().to_TEMP(x=PROD_SVOD, name="PROD_SVOD_PROGNOZ_TEMP.csv")
        return PROD_SVOD
    """функция за обработку данных"""
"""обработка пути продаж формирование, групировка таблиц"""



#NEW().Stavka_nds_Kanal()
#NEW().Finrez()
#NEW().Obnovlenie_error()
NEW().Obnovlenie()
#PROGNOZ().SALES_obrabotka()
#BOT().to_day()
#PROGNOZ().Sales_prognoz()

#NEW().Check_set()