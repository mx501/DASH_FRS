import psutil
import shutil
import xlsxwriter
from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time, date
from pandas.tseries.offsets import MonthBegin
import time as t
import os
import pandas as pd
import sys
import math
import gc
import requests
import datetime
import holidays
# from memory_profiler import profile
import numpy as np
import calendar
import holidays
#import bot_TELEGRAM as bot
import GOOGL as gg
from dateutil import parser
from dateutil import relativedelta
from dateutil import rrule
import winsound
import datetime
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

TY_GROP = 00

geo = "w"
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


class RENAME:
    def Rread(self, name_data, name_col, name):
        print("Загрузка справочника магазинов...")
        replacements = pd.read_excel("https://docs.google.com/spreadsheets/d/1SfuC2zKUFt6PQOYhB8EEivRjy4Dz-o4WDL-IR7CT3Eg/export?exportFormat=xlsx")
        """replacements = pd.read_excel(PUT + "Справочники\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")"""
        rng = len(replacements)
        for i in range(rng): name_data[name_col] = \
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

    def TY_Spravochnik(self):
        ty = pd.read_excel("https://docs.google.com/spreadsheets/d/1qXyD0hr1sOzoMKvMyUBpfTXDwLkh0RwLcNLuiNbWmSM/export?exportFormat=xlsx")
        ty = ty[["!МАГАЗИН!","Менеджер"]]
        ty  = ty .rename(columns={"!МАГАЗИН!": "магазин"})
        return ty
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
class BOT:
    def bot_mes_html_TY(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'Bot\\key\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        #test = keys_dict.get('test')
        if TY_GROP == 1:
            TY_id = keys_dict.get('TY_id')
            """url = f'https://api.telegram.org/bot{token}/sendMessage'
            # TEST ####################################################
            # Параметры запроса для отправки сообщения
            data = {'chat_id': test, 'text': mes, 'parse_mode': 'HTML'}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response = requests.post(url, data=data)
            # Проверка ответа от сервера Telegram
            if response.status_code == 200:
                print('Отправлено Test')
            else:
                print(f'Ошибка при отправке Test: {response.status_code}')"""

            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params_ty = {'chat_id': TY_id, 'text': mes, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response_ty = requests.post(url, data=params_ty)
            # Проверка ответа от сервера Telegram
            if response_ty.status_code == 200:
                print('Сообщение успешно Территориалов!')
            else:
                print(f'Ошибка при отправке Территориалов: {response_ty.status_code}')
    """отправка сообщений d в формате HTML"""
    def bot_mes(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'Bot\\key\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('test')
        #TY_id = keys_dict.get('TY_id')
        #analitik = keys_dict.get('analitik')
        #BOT_RUK_FRS = keys_dict.get('BOT_RUK_FRS')
        # TEST ####################################################
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Параметры
        params = {'chat_id': test, 'text':mes,}
        # ЗАПРОС ОТПРАВКА
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram
        if response.status_code == 200:
            print('Отправлено Test')
        else:
            print(f'Ошибка при отправке Test: {response.status_code}')
        """if TY_GROP == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params_ty = {'chat_id': TY_id, 'text': mes }
            # Отправка запроса на сервер Telegram для отправки сообщения
            response_ty = requests.post(url, data=params_ty)
            # Проверка ответа от сервера Telegram
            if response_ty.status_code == 200:
                print('Сообщение успешно Руководители!')
            else:
                print(f'Ошибка при отправке Группа руководители: {response_ty.status_code}')"""
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
class CustomRusHolidays(holidays.RU):
    def _populate(self, year,):
        super()._populate(year)
        # Добавляем в наш пользовательский набор праздников все официальные выходные дни.
        self[date(year, 5, 6)] = "День Воинской славы России"
        self[date(year, 5, 7)] = "День Воинской славы России"
        self[date(year, 5, 8)] = "День Победы"
        self[date(year, 5, 9)] = "День Победы"
class BOT_raschet:

    def tovar_day(self):
        return
    # отвечает за товар дня
    def tabl_bot_date(self):
        # определение рабочего дня или выходного
        def is_workday(date):
            ru_holidays = CustomRusHolidays()
            if date.weekday() >= 5:  # Если это суббота или воскресенье, то это выходной день.
                return False
            elif date in ru_holidays:  # Если это праздничный день, то это выходной день.
                return False
            else:
                return True  # Иначе это рабочий день.
        def save_date(date_list,name):
            with open(PUT + "Bot\\temp\\даты файлов\\" + name + '.txt', 'w') as f:
                f.write(str(date_list))

        # Чтение даты из файла
        with open(PUT + 'NEW\\дата обновления.txt', 'r') as f:
            date_str = f.readline().strip()
        format_date_str = '%d.%m.%Y'
        # Дата обновления
        MAX_DATE = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        TODEY = [MAX_DATE.strftime(format_date_str)]
        LAST_DATE = MAX_DATE - datetime.timedelta(days=1)
        print("Дата в файле\n",TODEY)

        # тестовая
        test = 1
        if test ==1:
            MAX_DATE = datetime.datetime.strptime("2023-05-10", '%Y-%m-%d').date()
            LAST_DATE = MAX_DATE - datetime.timedelta(days=1)

        # ФОРМИРОВАНИЕ СПИСКА ВЧЕРАШНЕЙ ДАТЫ
        priznzk = ""
        VCHERA= []
        if is_workday(MAX_DATE):
            priznzk = "рабочий день"
            if is_workday(LAST_DATE):
                priznzk = 'середина недели'
                VCHERA.append(LAST_DATE.strftime(format_date_str))
            else:
                priznzk = "начало недели"
                while not is_workday(LAST_DATE):
                    VCHERA.append(LAST_DATE.strftime(format_date_str))
                    LAST_DATE -= datetime.timedelta(days=1)
                VCHERA.append(LAST_DATE.strftime(format_date_str))
        else:
            priznzk = "выходной день"
        # запись в файл
        print(priznzk)
        print(VCHERA)

        # region ТЕКУШИЙ МЕСЯЦ
        # Определяем первый день текущего месяца
        TODEY_month_min_day = MAX_DATE.replace(day=1)
        # список дат
        TODEY_month = pd.date_range(start=TODEY_month_min_day, end=MAX_DATE  - datetime.timedelta(days=1), freq='D').strftime(format_date_str).tolist()
        print("Текущий месяц\n",TODEY_month)
        # endregion

        # region ПРОШЛЫЙ МЕСЯЦ
        LAST_month_min_day = TODEY_month_min_day - pd.offsets.MonthBegin(1)
        # Определяем последний день прошлого месяца
        LAST_month_max_day = TODEY_month_min_day - pd.offsets.Day(1)
        # Создаем список дат прошлого месяца
        LAST_month = pd.date_range(start=LAST_month_min_day, end=LAST_month_max_day, freq='D').strftime(format_date_str).tolist()

        # Определяем количество дней в каждом месяце
        days_in_today_month = len(TODEY_month)
        days_in_last_month = len(LAST_month)
        # Если количество дней в прошлом месяце больше, отфильтруем его, чтобы было равное количество дней
        if days_in_last_month > days_in_today_month:
            LAST_month = LAST_month[:days_in_today_month]
        print("Прошлый месяц\n",LAST_month)

        # endregion
        save_date(priznzk, "priznzk")
        save_date(TODEY,"TODEY")
        save_date(VCHERA,"VCHERA")
        save_date(TODEY_month,"TODEY_month")
        save_date(LAST_month,"LAST_month")

        return TODEY, VCHERA, TODEY_month, LAST_month, priznzk
    # формирование списка дат
    def tabl_bot_file(self):
        TODEY, VCHERA, TODEY_month, LAST_month, priznzk = BOT_raschet().tabl_bot_date()


        Bot = pd.DataFrame()
        def col_n(x):
            if "списания" not in x.columns:
                # если нет, то создаем столбец "списания"
                x["списания"] = 0
            if "операция" not in x.columns:
                # если нет, то создаем столбец "списания"
                x["операция"] = 0
            len_float =["выручка","скидка"]
            FLOAT().float_colms(name_data=x, name_col=len_float)
            x.loc[x["выручка"] > 0, "операция"] = "Выручка"
            x.loc[x["скидка"] > 0, "операция"] = "Скидка"
            x.loc[x["операция"] == "Дегустации", "Дегустации"] = x["списания"]
            x.loc[x["операция"] == "Хозяйственные товары", "Хозяйственные товары"] = x["списания"]
            x.loc[(x["операция"] == "Кражи")
                    | (x["операция"] == "ПОТЕРИ")
                    | (x["операция"] == "Питание сотрудников")
                    | (x["операция"] == "Подарок покупателю (сервисная фишка)")
                    | (x["операция"] == "Подарок покупателю (бонусы)")
                    | (x["операция"] == "Дегустации") | (x["операция"] == "МАРКЕТИНГ (блогеры, фотосессии)"), "Списания_показатель"] = x["списания"]
            return x

        # создание столбцов для отбора
        def poisk(file,otbor):
            file_p = file + '.xlsx'
            folder1 = PUT + "↓ТЕКУЩИЙ МЕСЯЦ\\Продажи текущий месяц\\"
            folder2 = PUT + "↓АРХИВ для дш\\Продажи\\Архив\\2023\\"
            for folder in [folder1, folder2]:

                file_path = os.path.join(folder, file_p)
                if os.path.exists(file_path):
                    print(file_path)
                    x = pd.read_excel(file_path, parse_dates=["Дата/Время чека"], date_format='%Y-%m-%d %H:%M:%S')
                    y = x[["Дата/Время чека","!МАГАЗИН!","номенклатура_1с","Стоимость позиции","Сумма скидки","операции","сумма_списания"]]
                    del x
                    gc.collect()
                    # перименование столбцов
                    y = y.rename(columns={"!МАГАЗИН!":"магазин","номенклатура_1с":"номенклатура",
                                          "Стоимость позиции":"выручка","Сумма скидки":"скидка","Дата/Время чека":"дата","операции":"операция","сумма_списания":"списания"})


                    # создание столбца отюора
                    y["отбор"] = otbor
                    col_n(y)

                    # перевод во float
                    len_float = ["выручка","скидка","списания","Дегустации","Хозяйственные товары","Списания_показатель"]
                    FLOAT().float_colms(name_data=y,name_col=len_float)
                    # групировка таблицы
                    y= y.groupby(["магазин","номенклатура","отбор","операция"],
                                  as_index=False).agg(
                        {"выручка": "sum", "скидка": "sum", "списания": "sum", "Дегустации": "sum", "Хозяйственные товары": "sum",
                         "Списания_показатель": "sum"}).reset_index(drop=True)




                    return y

        for file in TODEY:
            X = poisk(file=str(file), otbor="TODEY")
            print(X)
            Bot = pd.concat([Bot, X], axis=0,).reset_index(drop=True)
            del file
            gc.collect()
            MEMORY().mem_total(x="TODEY")

        for file in VCHERA:
            X = poisk(file=str(file), otbor="VCHERA")
            Bot = pd.concat([Bot, X], axis=0,).reset_index(drop=True)
            del file
            gc.collect()
            MEMORY().mem_total(x="VCHERA")

        for file in TODEY_month:
            X = poisk(file=str(file), otbor="TODEY_month")
            Bot = pd.concat([Bot, X], axis=0,).reset_index(drop=True)
            del file
            gc.collect()
            MEMORY().mem_total(x="TODEY_month")

        for file in LAST_month:
            X = poisk(file=str(file), otbor="LAST_month")
            Bot = pd.concat([Bot, X], axis=0,).reset_index(drop=True)
            del file
            gc.collect()
            MEMORY().mem_total(x="LAST_month")

        # Добавление ТУ
        MEMORY().mem_total(x="3")
        ty = RENAME().TY_Spravochnik()
        Bot = Bot.merge(ty, on=["магазин"], how="left").reset_index(drop=True)
        del ty,
        gc.collect()

        # переисенование менеджеров
        Ln_tip = {'Турова Анна Сергеевна': 'Турова А.С',
                  'Баранова Лариса Викторовна': 'Баранова Л.В',
                  'Геровский Иван Владимирович': 'Геровский И.В',
                  'Изотов Вадим Валентинович': 'Изотов В.В',
                  'Томск': 'Томск',
                  'Павлова Анна Александровна': 'Павлова А.А',
                  'Бедарева Наталья Геннадьевна': 'Бедарева Н.Г',
                  'Сергеев Алексей Сергеевич': 'Сергеев А.С',
                  'Карпова Екатерина Эдуардовна': 'Карпова Е.Э'}
        Bot["Менеджер"] = Bot["Менеджер"].map(Ln_tip)

        Bot.to_excel(PUT + "Bot\\temp\\" + "Bot_v2test.xlsx", index=False)
        return Bot
    # создание таблиц
    def raschet(self):
        def DATE():

            # Определение даты обновления дашборда
            now = datetime.now()
            NEW_date = (now.hour + 1) if now.minute >= 30 else (now.hour)
            NEW_date = datetime(now.year, now.month, now.day, NEW_date, 0, 0)
            NEW_date = NEW_date.strftime("%H:%M")
            print("Текущее время (округлено до часа):", NEW_date)
            current_time = f'🕙 Данные на : {NEW_date}\n'

            # список дат из файла TODEY_month
            with open(PUT + "Bot\\temp\\даты файлов\\TODEY.txt", 'r') as f:
                dates = f.read().strip()[1:-1].split(', ')

            # Формируем сообщение TODEY_month
            TODEY_date = f'Результаты прошлого дня:\n'
            for date in dates:
                TODEY_date +=  f'•\u200E {date[1:-1]}\n'
            print(TODEY_date)

            # список дат из файла TODEY_month
            with open(PUT + "Bot\\temp\\даты файлов\\VCHERA.txt", 'r') as f:
                dates = f.read().strip()[1:-1].split(', ')

            # Формируем сообщение TODEY_month
            VCHERA_date = f'Результаты прошедших выходных:\n'
            for date in dates:
                VCHERA_date += f'•\u200E {date[1:-1]}\n'
            print(VCHERA_date)

            return VCHERA_date,TODEY_date

        DATE()



        #now = datetime.now()
        #current_time = now.strftime("%H:%M:%S")
        #f = "10:00:00"
        #df = pd.read_excel(PUT + "Bot\\temp\\" + "Сводная_бот.xlsx")




        return





    def BOT(self):
        #########################Товар дня
        TOVAR_DEY = pd.read_excel("https://docs.google.com/spreadsheets/d/1oDZQWMkKWHP4SBjZD4GYRWjZYeH1AUjRvH2z1Ik3T1g/export?exportFormat=xlsx",)
        keys_dict = dict(zip(TOVAR_DEY.iloc[:, 0], TOVAR_DEY.iloc[:, 1]))
        N1 = keys_dict.get('n1')
        t2 = keys_dict.get('test')
        def col_n(x):
            # создание столбцов для отбора
            if 'операции' not in x.columns:
                # если нет, то создаем столбец "операции"
                df.insert(column='операции', value='')
            if 'сумма_списания' not in x.columns:
                # если нет, то создаем столбец "операции"
                df.insert(column='сумма_списания', value=0)
            x = x.rename(columns={"Стоимость позиции": "Выручка"})
            x.loc[x["операции"] == "Дегустации", "Дегустации"] = x["сумма_списания"]
            x.loc[x["операции"] == "Хозяйственные товары", "Хозяйственные товары"] = x["сумма_списания"]
            x.loc[(x["операции"] == "Кражи")
                    | (x["операции"] == "ПОТЕРИ")
                    | (x["операции"] == "Питание сотрудников")
                    | (x["операции"] == "Подарок покупателю (сервисная фишка)")
                    | (x["операции"] == "Подарок покупателю (бонусы)")
                    | (x["операции"] == "Дегустации") | (x["операции"] == "МАРКЕТИНГ (блогеры, фотосессии)"), "Списания"] = x["сумма_списания"]

            return x
        def plan():
            plan = pd.read_excel("C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Планы\\Планы ДЛЯ ДАШБОРДА.xlsx",parse_dates=["дата"], date_format='%d.%m.%Y')
            FLOAT().float_colm(name_data=plan, name_col="ПЛАН")
            plan["Месяц"] = plan["дата"].dt.month
            # Расчет месячного плана
            plan_sales_month = plan.loc[(plan["Месяц"] == max_mounth) & (plan["Показатель"] == "Выручка") & (plan["Тип "] == "АКТУАЛЬНЫЕ")]
            del plan
            plan_sales = plan_sales_month[["!МАГАЗИН!","ПЛАН","Месяц"]]
            # расчет дневного плана



            return plan_sales
        def fil_pisk(file,priznak):
            print("__________________________________________________________",priznak)
            file_p = file + '.xlsx'
            folder1 = PUT + "↓ТЕКУЩИЙ МЕСЯЦ\\Продажи текущий месяц\\"
            folder2 = PUT + "↓АРХИВ для дш\\Продажи\\Архив\\2023\\"
            for folder in [folder1, folder2]:
                file_path = os.path.join(folder, file_p)
                if os.path.exists(file_path):
                   #print(f"Файл {file_p} найден в папке {folder}.")
                    print(file_path)
                    x = pd.read_excel(file_path, parse_dates=["Дата/Время чека"],date_format='%Y-%m-%d %H:%M:%S')
                    print(priznak, "\n", x)
                    x = x[["Дата/Время чека", "!МАГАЗИН!", "Стоимость позиции", "Сумма скидки", "операции", "сумма_списания", 'номенклатура_1с']]
                    x["Фильтр время"] = priznak
                    x["Месяц"] = x["Дата/Время чека"].dt.month
                    x["Год"] = x["Дата/Время чека"].dt.year
                    x["День"] = x["Дата/Время чека"].dt.day
                    x["Дата/Время чека"] = pd.to_datetime(x["Дата/Время чека"], format='%Y-%m-%d')
                    x["Day"] = x["Дата/Время чека"].dt.day
                    with open(PUT + 'Bot\\temp\\max_date.txt', 'r') as f:
                        max_date_ = f.read().strip()
                        print(max_date_)
                        max_date_DAY = datetime.strptime(max_date_, '%Y-%m-%d %H:%M:%S').day
                        max_date_ = datetime.strptime(max_date_, '%Y-%m-%d %H:%M:%S')

                        print(max_date_DAY)
                    if priznak == "сегодня":
                        x = x

                    if  priznak == "ВЧЕРАШНЯЯ ДАТА":
                        x = x
                        """x = x.loc[x["Дата/Время чека"] == pd.to_datetime(max_date_, format='%d.%m.%Y') - pd.offsets.Day(1)]
                        x = x.loc[x["Дата/Время чека"] == pd.to_datetime(max_date_, format='%d.%m.%Y') - pd.offsets.Day(2)]
                        x = x.loc[x["Дата/Время чека"] == pd.to_datetime(max_date_, format='%d.%m.%Y') - pd.offsets.Day(3)]"""
                    if priznak == "ПРОШЛЫЙ МЕСЯЦ":
                        x = x.loc[x["День"] < max_date_DAY]
                    if priznak == "ТЕКУШИЙ МЕСЯЦ":
                        x = x.loc[x["День"] < max_date_DAY]


                    # роисаоение форматов
                    ln = ("Стоимость позиции", "сумма_списания", "Сумма скидки")
                    FLOAT().float_colms(name_data=x, name_col=ln)
                    x.loc[x["Стоимость позиции"] > 0, "операции"] = "Выручка"
                    x.loc[x["Сумма скидки"] > 0, "операции"] = "Скидка"

                    x = col_n(x=x)

                    x = x.groupby(["Фильтр время", 'номенклатура_1с', "!МАГАЗИН!", "Месяц", "Год"],
                                    as_index=False).agg(
                        {"Выручка": "sum", "сумма_списания": "sum", "Сумма скидки": "sum", "Дегустации": "sum", "Хозяйственные товары": "sum",
                         "Списания": "sum"}).reset_index(drop=True)
                    print(priznak,"\n",x)
                    return x


        ##########################
        # region ПОИСК МАКСИМАЛЬНОЙ ДАТЫ
        max_date = datetime.min  # установим начальное значение для максимальной даты

        for filename in os.listdir(PUT + "↓ТЕКУЩИЙ МЕСЯЦ\\Продажи текущий месяц\\"):
                try:
                    file_date = datetime.strptime(filename[:-5], '%d.%m.%Y')  # извлекаем дату из названия файла
                    if file_date > max_date:
                        max_date = file_date  # обновляем максимальную дату, если нужно
                except ValueError:
                    pass  # если формат даты некорректный, игнорируем файл
        # дата максимальная в формате названия файла
        date_obj = datetime.strptime(str(max_date), '%Y-%m-%d %H:%M:%S')
        file_max_date = date_obj.strftime('%d.%m.%Y')
        print("sdfsdfsdf", file_max_date)


        # endregion
        # максимальный год
        max_year = max_date.year
        # максимальный месяц
        max_mounth = max_date.month
        # максимальный день
        max_day = max_date.day


        with open(PUT + 'Bot\\temp\\max_date.txt', 'w') as f:
            f.write(str(date_obj))

        Bot = pd.DataFrame()
        # region СЕГОДНЯШНЯЯ ДАТА
        TODEY_date_file = pd.to_datetime(file_max_date, format='%d.%m.%Y').strftime('%d.%m.%Y')
        BOT().bot_mes(mes="СЕГОДНЯШНЯЯ ДАТА:\n " + str(TODEY_date_file))
        #TODEY_date_Todey = pd.to_datetime(file_max_date, format='%d.%m.%Y')
        file = str(TODEY_date_file)
        TODEY = fil_pisk(file=file, priznak="сегодня")
        """TODEY = pd.read_excel(PUT + "↓ТЕКУЩИЙ МЕСЯЦ\\Продажи текущий месяц\\" + str(TODEY_date_file) + '.xlsx', parse_dates=["Дата/Время чека"],
                           date_format='%Y-%m-%d %H:%M:%S')

        TODEY["Фильтр время"] = "сегодня"
        TODEY["Месяц"] = TODEY["Дата/Время чека"].dt.month
        TODEY["Год"] = TODEY["Дата/Время чека"].dt.year
        TODEY["День"] = TODEY["Дата/Время чека"].dt.day
        TODEY = TODEY[["Дата/Время чека","!МАГАЗИН!", "Стоимость позиции",'номенклатура_1с', "Сумма скидки","Месяц","Год","День"]]
        TODEY["Фильтр время"] = "сегодня"
        # роисаоение форматов
        ln = ("Стоимость позиции",  "Сумма скидки")
        FLOAT().float_colms(name_data=TODEY, name_col=ln)
        TODEY = TODEY.rename(columns={"Стоимость позиции": "Выручка"})

        TODEY = TODEY.groupby(["Фильтр время", 'номенклатура_1с', "!МАГАЗИН!", "Месяц", "Год"],
                                        as_index=False).agg(
            {"Выручка": "sum", "Сумма скидки": "sum"}).reset_index(drop=True)"""
        Bot = pd.concat([Bot, TODEY ], axis=0, ).reset_index(drop=True)
        del  TODEY,file
        gc.collect()
        MEMORY().mem_total(x="1")


        # endregion
        # region вЧЕРАШНЯЯ ДАТА
        TODEY_Last = pd.to_datetime(file_max_date, format='%d.%m.%Y') - pd.offsets.Day(1)

        # для выходных
        TODEY_Last1 = pd.to_datetime(file_max_date, format='%d.%m.%Y') - pd.offsets.Day(2)
        TODEY_Last1 = TODEY_Last.strftime('%d.%m.%Y')
        TODEY_Last2 = pd.to_datetime(file_max_date, format='%d.%m.%Y') - pd.offsets.Day(3)
        TODEY_Last2 = TODEY_Last.strftime('%d.%m.%Y')
        TODEY_Last3 = pd.to_datetime(file_max_date, format='%d.%m.%Y') - pd.offsets.Day(4)
        TODEY_Last3 = TODEY_Last.strftime('%d.%m.%Y')

        TODEY_Last = TODEY_Last.strftime('%d.%m.%Y')


        BOT().bot_mes(mes="Дата вчера:\n " + str(TODEY_Last))
        file = [str(TODEY_Last)]

        file.append(TODEY_Last1)
        file.append(TODEY_Last2)
        file.append(TODEY_Last3)
        for file in file:
            df = fil_pisk(file=file,priznak="ВЧЕРАШНЯЯ ДАТА")
            Bot = pd.concat([Bot, df], axis=0, ).reset_index(drop=True)
        del TODEY_Last,df
        gc.collect()
        #df = fil_pisk(file=file, priznak="ВЧЕРАШНЯЯ ДАТА")
        # ###############################################################################################################################################
        #Bot = pd.concat([Bot, df], axis=0, ).reset_index(drop=True)

        # endregion
        # region ПРОШЛЫЙ МЕСЯЦ
        # Преобразуем строку в объект datetime
        file_max_date_ln = pd.to_datetime(file_max_date, format='%d.%m.%Y')
        # Определяем первый день текущего месяца
        first_day_of_month = file_max_date_ln.replace(day=1)
        # Определяем первый день прошлого месяца
        first_day_of_last_month = first_day_of_month - pd.offsets.MonthBegin(1)
        # Определяем последний день прошлого месяца
        last_day_of_last_month = first_day_of_month - pd.offsets.Day(1)
        # Создаем список дат прошлого месяца
        dates_of_last_month = pd.date_range(start=first_day_of_last_month, end=last_day_of_last_month, freq='D').strftime('%d.%m.%Y').tolist()
        # Фильтруем даты по условию "меньше file_max_date"
        ln_mount_proshl = [date for date in dates_of_last_month if pd.to_datetime(date, format='%d.%m.%Y') < file_max_date_ln]
        BOT().bot_mes(mes="Прошлый месяц:\n " + "Мин: " + str(first_day_of_last_month)[:-9] +  "\nМин: " + str(last_day_of_last_month)[:-9])

        for file in ln_mount_proshl:
            df = fil_pisk(file = file,priznak="ПРОШЛЫЙ МЕСЯЦ")
            Bot = pd.concat([Bot, df], axis=0,).reset_index(drop=True)
            del df,file
            gc.collect()
            MEMORY().mem_total(x="прошлый после удаления")
        # endregion

        # region ТЕКУШИЙ МЕСЯЦ
        # строку в объект datetime
        file_max_date_ln = pd.to_datetime(file_max_date, format='%d.%m.%Y')
        #file_max_date_ln = file_max_date_ln - pd.offsets.Day(1)
        # Определяем первый день текущего месяца
        first_day_of_month = file_max_date_ln.replace(day=1)
        # список дат
        dates_of_last_month = pd.date_range(start=first_day_of_month , end=file_max_date_ln, freq='D').strftime('%d.%m.%Y').tolist()
        print(dates_of_last_month)
        # Фильтруем даты по условию "меньше file_max_date"
        ln_mount_tec = [date for date in dates_of_last_month if pd.to_datetime(date, format='%d.%m.%Y')]
        print(dates_of_last_month)
        BOT().bot_mes(mes="Текущий месяц:\n " + "Мин: " + str(first_day_of_month)[:-9] + "\nМин: " + str(file_max_date_ln)[:-9])


        for file in ln_mount_tec:
            df = fil_pisk(file=file,priznak="ТЕКУШИЙ МЕСЯЦ")
            Bot = pd.concat([Bot, df], axis=0, ).reset_index(drop=True)
            del df,file
            gc.collect()
            MEMORY().mem_total(x="текуищий после удаления")

        # endregion


        # Добавление ТУ
        MEMORY().mem_total(x="3")
        ty = RENAME().TY_Spravochnik()
        Bot = Bot.merge(ty, on=["!МАГАЗИН!"], how="left").reset_index(drop=True)
        del ty,
        gc.collect()

        # переисенование менеджеров
        Ln_tip = {'Турова Анна Сергеевна':'Турова А.С',
            'Баранова Лариса Викторовна': 'Баранова Л.В',
            'Геровский Иван Владимирович': 'Геровский И.В',
            'Изотов Вадим Валентинович': 'Изотов В.В',
            'Томск': 'Томск',
            'Павлова Анна Александровна': 'Павлова А.А',
            'Бедарева Наталья Геннадьевна': 'Бедарева Н.Г',
            'Сергеев Алексей Сергеевич':'Сергеев А.С',
            'Карпова Екатерина Эдуардовна': 'Карпова Е.Э'}
        Bot["Менеджер"] = Bot["Менеджер"].map(Ln_tip)


        ############################### Товар дня
        TOVAR_DAY= Bot.loc[Bot["номенклатура_1с"] == N1]
        ###############################
        print(Bot)
        Bot = Bot.groupby(["Фильтр время", "!МАГАЗИН!", "Месяц", "Год","Менеджер"],
                          as_index=False).agg({"Выручка": "sum", "сумма_списания": "sum", "Сумма скидки": "sum","Дегустации": "sum","Хозяйственные товары": "sum","Списания": "sum"}).reset_index(drop=True)
        Bot.to_excel(PUT + "Bot\\temp\\" + "Сводная_бот.xlsx", index=False)
        MEMORY().mem_total(x="Память бот")
        del Bot
        gc.collect()
        TOVAR_DAY.to_excel(PUT + "Bot\\temp\\" + "Сводная_бот_товар_дня.xlsx", index=False)
        TOVAR_DAY =TOVAR_DAY.groupby(["Фильтр время",'номенклатура_1с', "!МАГАЗИН!", "Месяц", "Год","Менеджер"],
                          as_index=False).agg({  "Выручка": "sum","сумма_списания": "sum","Сумма скидки": "sum","Дегустации": "sum","Хозяйственные товары": "sum","Списания": "sum"}).reset_index(drop=True)
        TOVAR_DAY.to_excel(PUT + "Bot\\temp\\" + "Сводная_бот_товар_дня.xlsx", index=False)
        del TOVAR_DAY
        gc.collect()
        MEMORY().mem_total(x="4")
        """Bot.to_csv(PUT + "Bot\\temp\\" + "Сводная_бот.csv", encoding="ANSI", sep=';',
                 index=False, decimal=',')"""


        BOT().bot_mes(mes="Файл для бота обработан")
        BOT_raschet().Messege()
    def Messege(self):
        with open(PUT + 'Bot\\temp\\max_date.txt', 'r') as f:
            max_date = f.read().strip()
            max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        f = "10:00:00"
        df = pd.read_excel(PUT + "Bot\\temp\\" + "Сводная_бот.xlsx")
        print(df)
        # region ТЕРРИТОРИАЛЫ
        # получение списка териториалов
        TY_LIST = df.iloc[1:, 4].unique().tolist()
        # исключение из списка териториалов
        TY_LIST = [item for item in TY_LIST if item not in ['закрыт', 'нет магазина']]
        """Турова Анна Сергеевна':'Турова А.С',
            'Баранова Лариса Викторовна': 'Баранова Л.В',
            'Геровский Иван Владимирович': 'Геровский И.В', TY_LIST = ['Геровский И.В','Турова А.С']  """


        if  current_time<f:

            """ВЫЧИСЛЕНИЯ ДЛЯ ПРОШЛОГО ДНЯ"""
            for i in TY_LIST:
                    t.sleep(10)

                    MES_TEC = (df["Фильтр время"] == "ТЕКУШИЙ МЕСЯЦ")
                    MES_prosh = (df["Фильтр время"] == "ПРОШЛЫЙ МЕСЯЦ")
                    VCHERA = (df["Фильтр время"] == "ВЧЕРАШНЯЯ ДАТА")
                    # формирование гугл таблиц вчерашний день
                    Googl_tbl = df.loc[(df["Менеджер"] == i) & VCHERA]
                    Googl_tbl = Googl_tbl[["!МАГАЗИН!","Менеджер","Выручка","Списания"]]
                    Goole_url = gg.tbl().record(name=i + "_Прошлый день", name_df=Googl_tbl)
                    del Googl_tbl
                    # формирование гугл таблиц прошлый месяц
                    Googl_tbl_mes = df.loc[(df["Менеджер"] == i) & MES_TEC]
                    Googl_tbl_mes = Googl_tbl_mes[["!МАГАЗИН!", "Менеджер", "Выручка", "Списания"]]
                    Goole_url_mes = gg.tbl().record(name= i +"_Текущий месяц", name_df=Googl_tbl_mes)
                    del Googl_tbl_mes
                    gc.collect()

                    #max_date = max_date # df["Дата/Время чека"].max()
                    max_date_str = max_date.strftime('%Y-%m-%d')
                    # определение максимального дня название переименование в руские названия
                    weekday = datetime.strptime(max_date_str, '%Y-%m-%d').strftime('%A')
                    weekday_perevod = {
                        'Monday': 'Понедельник',
                        'Tuesday': 'Вторник',
                        'Wednesday': 'Среда',
                        'Thursday': 'Четверг',
                        'Friday': 'Пятница',
                        'Saturday': 'Суббота',
                        'Sunday': 'Воскресенье'}
                    weekday = weekday_perevod.get(weekday, 'День недели не найден')

                    podpis_mes = "Результаты прошлого дня:"
                    date_day = "   • " + max_date.strftime("%Y-%m-%d")
                    date_day_vcher = pd.to_datetime(max_date, format='%d.%m.%Y') - pd.offsets.Day(1)
                    date_day_vcher ="   • " + date_day_vcher.strftime("%Y-%m-%d")
                    # если выходные
                    date_day_vcher1 = pd.to_datetime(max_date, format='%d.%m.%Y') - pd.offsets.Day(2)
                    date_day_vcher1 = "   • " + date_day_vcher1.strftime("%Y-%m-%d")
                    date_day_vcher2 = pd.to_datetime(max_date, format='%d.%m.%Y') - pd.offsets.Day(3)
                    date_day_vcher2 = "   • " + date_day_vcher2.strftime("%Y-%m-%d")
                    date_day_vcher3 = pd.to_datetime(max_date, format='%d.%m.%Y') - pd.offsets.Day(4)
                    date_day_vcher3 = "   • " + date_day_vcher3.strftime("%Y-%m-%d")

                    max_date_mounth_mes = []
                    if weekday == 'Воскресенье':
                        VCHERA = (df["Дата/Время чека"] <= max_date) & (df["Дата/Время чека"] >= df["Дата/Время чека"].max() - pd.Timedelta(days=2))
                        podpis_mes = "Результаты прошедших выходных:"
                        min_date = max_date - pd.Timedelta(days=2)
                        date_day = "    • " + min_date.strftime("%Y-%m-%d") + " • " + max_date.strftime("%Y-%m-%d")

                    # region Переименование месяцов.
                    # определение максимального месяца
                    max_date_mounth = df["Месяц"].max()
                    # определение максимального года
                    max_date_year = df["Год"].max()
                    MONTHS = {1: 'январь',
                              2: 'февраль',
                              3: 'март',
                              4: 'апрель',
                              5: 'май',
                              6: 'июнь',
                              7: 'июль',
                              8: 'август',
                              9: 'сентябрь',
                              10: 'октябрь',
                              11: 'ноябрь',
                              12: 'декабрь'}
                    #max_date_mounth_mes = max_date_mounth.map(MONTHS)
                    max_date_mounth_mes = MONTHS.get(max_date_mounth, 'День недели не найден')

                    max_date_mounth_mes = "  • " + str(max_date_mounth_mes) + "  • " + str(max_date_year) + 'г'
                    # endregion


                    # проглый день #####################################################################################################
                    # Выручка за прошлый день
                    df_day_sales_f = df.loc[(df["Менеджер"] == i) & VCHERA]["Выручка"].sum()
                    df_day_sales = '{:,.0f}'.format(df_day_sales_f).replace(',', ' ')
                    """Списания показатель"""
                    # Списания за прошлый день
                    df_day_sp_f = df.loc[(df["Менеджер"] == i) & VCHERA]["Списания"].sum()
                    df_day_sp = '{:,.0f}'.format(df_day_sp_f).replace(',', ' ')
                    # % Списания за прошлый день
                    df_day_prosent_f = df_day_sp_f / df_day_sales_f
                    df_day_prosent = '{:,.1%}'.format(df_day_prosent_f).replace(',', ' ')
                    # у словия
                    sig_day_sp = "  • "
                    if df_day_prosent_f >= 0.025:
                        sig_day_sp = "   ❗"


                    # Списания ХОЗЫ ///добавить если макс воскресенье то брать 2 дня
                    df_day_sp_HOZ_f = df.loc[(df["Менеджер"] == i) & VCHERA ]["Хозяйственные товары"].sum()
                    df_day_sp_HOZ = '{:,.0f}'.format(df_day_sp_HOZ_f).replace(',', ' ')
                    # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
                    df_day_sp_HOZ_prosent = df_day_sp_HOZ_f / df_day_sales_f
                    df_day_sp_HOZ_prosent = '{:,.1%}'.format(df_day_sp_HOZ_prosent).replace(',', ' ')

                    # Списания Дегустации ///добавить если макс воскресенье то брать 2 дня
                    df_day_sp_DEG_f = df.loc[(df["Менеджер"] == i) & VCHERA]["Дегустации"].sum()
                    df_day_sp_DEG = '{:,.0f}'.format(df_day_sp_DEG_f).replace(',', ' ')
                    # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
                    df_day_sp_DEG_prosent = df_day_sp_DEG_f / df_day_sales_f
                    df_day_sp_DEG_prosent = '{:,.2%}'.format(df_day_sp_DEG_prosent).replace(',', ' ')

                    # CРЕДНИЙ ЧЕК


                    """ВЫЧСЛЕНИЯ ДЛЯ МЕСЯЦА"""
                    # Выручка текущий месяц
                    df_month_sales_f = df.loc[(df["Менеджер"] == i) & MES_TEC]["Выручка"].sum()
                    df_month_sales = '{:,.0f}'.format(df_month_sales_f).replace(',', ' ')
                    """Списания показатель"""
                    # Списания текущий месяц
                    df_month_sp_f = df.loc[(df["Менеджер"] == i) & MES_TEC]["Списания"].sum()
                    df_month_sp = '{:,.0f}'.format(df_month_sp_f).replace(',', ' ')
                    sig_month_sp = "  • "
                    if df_month_sp_f >= 0.025:
                        sig_month_sp = "   ❗"

                    # % Списания месяц
                    df_month_prosent = df_month_sp_f / df_month_sales_f
                    df_month_prosent = '{:,.1%}'.format(df_month_prosent).replace(',', ' ')

                    # Списания ХОЗЫ
                    df_month_sp_HOZ_f = df.loc[(df["Менеджер"] == i) & MES_TEC]["Хозяйственные товары"].sum()
                    df_month_sp_HOZ = '{:,.0f}'.format(df_month_sp_HOZ_f).replace(',', ' ')
                    # % Списания за месяц
                    df_month_sp_HOZ_prosent = df_month_sp_HOZ_f / df_month_sales_f
                    df_month_sp_HOZ_prosent = '{:,.1%}'.format(df_month_sp_HOZ_prosent).replace(',', ' ')


                    # ИЗМЕНЕНИЕ К ПРОШЛОМУ ДНБ
                    #max_date_prl = max_date - pd.Timedelta(days=2)

                    IZMEN_DAY_F = df.loc[(df["Менеджер"] == i) & VCHERA]["Выручка"].sum()
                    IZMEN_DAY_F  = (df_day_sales_f - IZMEN_DAY_F) / IZMEN_DAY_F
                    IZMEN_DAY = '{:,.1%}'.format(IZMEN_DAY_F).replace(',', ' ')
                    # ИЗМЕНЕНИЕ К ПРОШЛОМУ месяцу
                    IZMEN_M_F = df.loc[(df["Менеджер"] == i) & MES_prosh]["Выручка"].sum()
                    IZMEN_M_F = (df_month_sales_f  - IZMEN_M_F) / IZMEN_M_F
                    IZMEN_M_ = '{:,.1%}'.format(IZMEN_M_F).replace(',', ' ')

                    # ИЗМЕНЕНИЕ К ПРОШЛОМУ ДНБ списания
                    #max_date_prl = df["Дата/Время чека"].max() - pd.Timedelta(days=2)

                    IZMEN_DAY_s = df.loc[(df["Менеджер"] == i)  & VCHERA]["Списания"].sum()
                    IZMEN_DAY_s = (df_day_sp_f - IZMEN_DAY_s) / IZMEN_DAY_s
                    IZMEN_DAYs = '{:,.1%}'.format(IZMEN_DAY_s).replace(',', ' ')
                    # ИЗМЕНЕНИЕ К ПРОШЛОМУ списания
                    IZMEN_M_s = df.loc[(df["Менеджер"] == i) & MES_prosh]["Списания"].sum()
                    IZMEN_M_s = (df_month_sp_f - IZMEN_M_s) / IZMEN_M_s
                    IZMEN_Ms = '{:,.1%}'.format(IZMEN_M_s).replace(',', ' ')



                    # region условия
                    """ДЛЯ ПРОШЛОГО ДНЯ"""
                    sig_day_DEG = "  • "
                    if df_day_sp_DEG_f <= 0:
                        df_day_sp_DEG = "Дегустаций не было"
                        sig_day_DEG = "❗"
                    # endregion
                    #max_date = df["Дата/Время чека"].max()
                    podpis_mes = "Результаты прошедших выходных:"
                    SVODKA = f'<b>👨‍💼 {i}:</b>\n\n' \
                             f'<b><a href="{Goole_url}">{podpis_mes}\n</a></b>'\
                             f'<i>{date_day_vcher}{date_day_vcher1}\n{date_day_vcher2}{date_day_vcher3}</i>\n\n' \
                             f'💰 Выручка: {df_day_sales}\n' \
                             f'💸 Списания(показатель):\n{sig_day_sp}{df_day_sp} ({df_day_prosent})\n' \
                             f'     <i>• Хозы: {df_day_sp_HOZ} ({df_day_sp_HOZ_prosent})</i>\n' \
                             f'   <i>{sig_day_DEG}Дегустации: {df_day_sp_DEG} ({df_day_sp_DEG_prosent})</i>\n' \
                             f'🧾 Средний чек: -----\n\n' \
                             f'<b><a href="{Goole_url_mes}">Текущий месяц(Без сегодня): </a></b>\n' \
                             f'<i>{max_date_mounth_mes}</i>\n\n' \
                             f'💰 Выручка: {df_month_sales}\n' \
                             f'💸 Списания(показатель):\n{sig_month_sp}{df_month_sp} ({df_month_prosent})\n' \
                             f'     <i>• Хозы: {df_month_sp_HOZ} ({df_month_sp_HOZ_prosent})</i>\n\n' \
                             #f'<b>Изменение к прошлому дню/месяцу:</b>\n' \
                             #f'💰 Выручка: ({IZMEN_DAY}) ({IZMEN_M_})\n' \
                             #f'💸 Списания(показатель): ({IZMEN_DAYs}) ({IZMEN_Ms})\n\n'

                    BOT().bot_mes_html(mes=SVODKA)
                    if TY_GROP == 1:
                        BOT().bot_mes_html_TY(mes=SVODKA)

        if current_time > f:
            # получаем текущее время
            now = datetime.now()
            # округляем до ближайшего часа
            rounded_hour = (now.hour + 1) if now.minute >= 30 else (now.hour)
            # создаем новое время, округленное до часа
            rounded_time = datetime(now.year, now.month, now.day, rounded_hour, 0, 0)
            # преобразуем в строку и выводим на экран
            current_time = rounded_time.strftime("%H:%M")
            print("Текущее время (округлено до часа):", current_time)
            current_time = f'🕙 Данные на : {current_time}\n'


            BOT().bot_mes_html(mes=current_time)
            if TY_GROP == 1:
                BOT().bot_mes_html_TY(mes=current_time)

            for i in TY_LIST:
                SEGOD  = (df["Фильтр время"] == "сегодня")
                t.sleep(10)
                # Выручка за сегодня
                df_day_sales_f = df.loc[(df["Менеджер"] == i) & SEGOD]["Выручка"].sum()
                df_day_sales = '{:,.0f}'.format(df_day_sales_f).replace(',', ' ')
                # Скидки за сегодня
                SKIDKI_TODEY_N= df.loc[(df["Менеджер"] == i) & SEGOD]["Сумма скидки"].sum()
                SKIDKI_TODEY = '{:,.0f}'.format(SKIDKI_TODEY_N).replace(',', ' ')
                SKIDKI_TODEY_PROC_N = SKIDKI_TODEY_N / df_day_sales_f
                SKIDKI_TODEY_PROC = '{:,.1%}'.format(SKIDKI_TODEY_PROC_N).replace(',', ' ')


                SVODKA = f'<b>👨‍ {i}:</b>\n' \
                         f'💰 Выручка : {df_day_sales}\n'\
                         f'🎁 Скидки : {SKIDKI_TODEY}  ({SKIDKI_TODEY_PROC})\n'

                BOT().bot_mes_html(mes=SVODKA)
                if TY_GROP == 1:
                    BOT().bot_mes_html_TY(mes=SVODKA)
BOT_raschet().tabl_bot_date()
BOT_raschet().raschet()
#BOT_raschet().tabl_bot_file()