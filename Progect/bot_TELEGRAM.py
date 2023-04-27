import psutil
import shutil
import xlsxwriter
from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time,date
from pandas.tseries.offsets import MonthBegin
import os
import pandas as pd
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
# region счет памяти
# счет памяти Dask
"""total_memory_usage = df.memory_usage(deep=True).sum().compute()
print("Total memory usage: {:.2f} GB".format(total_memory_usage / 1e9))"""
# счет памяти pandas
"""total_memory_usage = df.memory_usage(deep=True).sum()
print("Total memory usage: {:.2f} MB".format(total_memory_usage / 1e6))"""
gc.enable()
# endregion

# Отправлять ли в группу вечеринка аналитиков Сообщения?
BOT_ANALITIK = "n"
BOT_RUK = "n"
TY_GROP ="n"
# пересчитать данные
DATA = "n"

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

class MEMORY:
    def mem(self, x, text):
        total_memory_usage = x.memory_usage(deep=True).sum()
        print(text + " - Использовано памяти: {:.2f} MB".format(total_memory_usage / 1e6))

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
"""Переименовать магазины"""
class DOC:

    def to_CSV(self, x, name):
        x.to_csv(PUT + "TEMP\\BOT\\data\\" + name, encoding="utf-8", sep=';',
                 index=False, decimal='.')
"""Сохранение файлов"""
class OPENAI:
    def open_ai(self):
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
    def open_ai_curi(self, mes):
        #mes_bot = BOT().to_day()
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
        # Определение текста запроса
        request = mes
        #request = mes_bot
        # Форматирование текста
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=(f"составь красивое сообщение для телеграм:\n{request}\n\n"),
            max_tokens=500,
            temperature = 0.5)
        # Получение отформатированного текста
        formatted_text = response.choices[0].text.strip()

        # Вывод отформатированного текста
        BOT().bot_mes(mes=formatted_text)
        print(formatted_text)
    def GTPchat(self, mes):
        #mes_bot = BOT().to_day()
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
        # Определение текста запроса
        request = mes
        #request = mes_bot
        # Форматирование текста
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=(f"составь красивое сообщение для телеграм:\n{request}\n\n"),
            max_tokens=500,
            temperature = 0.5)
        # Получение отформатированного текста
        formatted_text = response.choices[0].text.strip()

        # Вывод отформатированного текста
        BOT().bot_mes(mes=formatted_text)
        print(formatted_text)
"""запрос к базе опен ai"""
class BOT:
    def bot_mes(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'Bot\\key\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('test')
        TY_id = keys_dict.get('TY_id')
        #analitik = keys_dict.get('analitik')
        #BOT_RUK_FRS = keys_dict.get('BOT_RUK_FRS')
        # TEST ####################################################
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Параметры
        params = {'chat_id': test, 'text':mes}
        # ЗАПРОС ОТПРАВКА
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram
        if response.status_code == 200:
            print('Отправлено Test')
        else:
            print(f'Ошибка при отправке Test: {response.status_code}')
        if TY_GROP == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params_ty = {'chat_id': TY_id, 'text': mes }
            # Отправка запроса на сервер Telegram для отправки сообщения
            response_ty = requests.post(url, data=params_ty)
            # Проверка ответа от сервера Telegram
            if response_ty.status_code == 200:
                print('Сообщение успешно Руководители!')
            else:
                print(f'Ошибка при отправке Группа руководители: {response_ty.status_code}')
    """отправка сообщений"""
    def bot_raschet(self):
        if DATA=="y":
            # Обновление данных
            # вычисление максимальной даты
            max_date = pd.Timestamp('1900-01-01')
            for root, dirs, files in os.walk(PUT_BOT):
                for file in files:
                    if file.endswith('.txt'):  # проверяем только csv файлы
                        filepath = os.path.join(root, file)
                        df = pd.read_csv(filepath, delimiter='\t',  encoding="utf-8", parse_dates=['По дням'], usecols=[ 'По дням'])
                        max_date = max(max_date, pd.to_datetime(df['По дням'], errors='coerce').max())
                        print(max_date)
                        del df
            max_year = max_date.year
            max_mounth = max_date.month
            max_day = max_date.day
            # Список всех файлов в папке и подпапках
            all_files = []
            for root, dirs, files in os.walk(PUT_BOT):
                if max_year-1 in dirs:
                    dirs.remove("2021")
                for file in files:
                    all_files.append(os.path.join(root, file))
            # Список таблиц с данными за текущий месяц
            df_bot_1 = pd.DataFrame()
            for file in all_files:
                df = pd.read_csv(file, encoding="utf-8",
                                 sep='\t',
                                 parse_dates=['По дням'],
                                 usecols=['Склад магазин.Наименование', 'Номенклатура', 'По дням', "Выручка", "операции списания", "СписРуб"],
                                 low_memory=False, dtype={'операции списания': 'object', 'СписРуб': 'object'})

                df = df.loc[(df['По дням'].dt.year == max_year - 1) & (df['По дням'].dt.month == max_mounth) & (df['По дням'].dt.day <= max_day) |
                            (df['По дням'].dt.year == max_year) & (df['По дням'].dt.month == max_mounth - 1) & (df['По дням'].dt.day <= max_day) |
                            (df['По дням'].dt.year == max_year) & (df['По дням'].dt.month == max_mounth)]

                PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                           "подарочная карта КМ 500 НОВАЯ",
                           "подарочная карта КМ 1000 НОВАЯ")
                for x in PODAROK:
                    df = df[~df['Номенклатура'].str.contains(x)]
                df = df.drop(columns={"Номенклатура"})

                l_mag = ("Микромаркет", "Экопункт", "Вендинг","Итого")
                for w in l_mag:
                    df = df[~df['Склад магазин.Наименование'].str.contains(w)]


                df["операции списания"] = df["операции списания"].fillna('продажа')
                # выполнить действия для датафрейма
                df_bot_1 = pd.concat([df_bot_1, df], axis=0, ignore_index=True)
                print("обьеденение" + file)
                del df
            ln = ("Выручка",'СписРуб')
            for e in ln:
                df_bot_1[e] = (df_bot_1[e].astype(str)
                               .str.replace("\xa0", "")
                               .str.replace(",", ".")
                               .fillna("0")
                               .astype("float")
                               .round(2))
            df_bot_1 = df_bot_1.groupby(['По дням', 'Склад магазин.Наименование', "операции списания"]).sum().reset_index()
            df_bot_1 = df_bot_1.rename(columns={'Склад магазин.Наименование': 'магазин'})
            # загрузка файла справочника териториалов
            ty = pd.read_excel("https://docs.google.com/spreadsheets/d/1rwsBEeK_dLdpJOAXanwtspRF21Z3kWDvruani53JpRY/export?exportFormat=xlsx")
            ty = ty[["Название 1 С (для фин реза)", "Менеджер"]]

            rng, replacements = RENAME().Rread()
            for i in tqdm(range(rng), desc="ПереименованиеСписок ТУ - ", colour="#808080"): ty["Название 1 С (для фин реза)"] = \
                ty["Название 1 С (для фин реза)"].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)

            ty = ty.rename(columns={"Название 1 С (для фин реза)": 'магазин'})

            df_bot_1 = pd.merge(df_bot_1, ty, on=['магазин'], how='left')
            del ty
            df_bot_1.to_csv(PUT + "TEMP\\BOT\\data\\test.csv", encoding="ANSI", sep=';',
                            index=False, decimal='.')

            MEMORY().mem(x=df_bot_1, text="1")
            del df_bot_1
            gc.collect()
        # Чтение файла
        df = pd.read_csv(PUT + "TEMP\\BOT\\data\\test.csv", sep=';', encoding="ANSI", parse_dates=['По дням'])
        # получение списка териториалов
        TY_LIST = df.iloc[1:, 5].unique().tolist()

        # исключение из списка териториалов
        TY_LIST = [item for item in TY_LIST if item not in ['закрыт', 'нет магазина']]

        # ОПЕРАЦИИ С ДАТАМИ
        # определение максимальной даты приведение в формат
        max_date = df["По дням"].max()
        max_date_str = max_date.strftime('%Y-%m-%d')
        # определение максимального дня название переименование в руские названия
        weekday = datetime.strptime(max_date_str, '%Y-%m-%d').strftime('%A')
        weekday_perevod= {
            'Monday': 'Понедельник',
            'Tuesday': 'Вторник',
            'Wednesday': 'Среда',
            'Thursday': 'Четверг',
            'Friday': 'Пятница',
            'Saturday': 'Суббота',
            'Sunday': 'Воскресенье'}
        weekday = weekday_perevod.get(weekday, 'День недели не найден')
        # определение максимального месяца
        df["месяц"] = df["По дням"].dt.month
        max_date_mounth =df["месяц"].max()
        # определение максимального года
        df["год"] = df["По дням"].dt.year
        max_date_year = df["год"].max()

        filter_date_day = (df["По дням"] == max_date)
        podpis_mes = "Результаты прошлого дня:"
        date_day ="   • " + max_date.strftime("%Y-%m-%d")
        if weekday == 'Воскресенье':
            filter_date_day = (df["По дням"] <= max_date) & (df["По дням"] >= df["По дням"].max() - pd.Timedelta(days=1))
            podpis_mes = "Результаты прошедших выходных:"
            min_date = df["По дням"].max() - pd.Timedelta(days=1)
            date_day = "    • " + min_date.strftime("%Y-%m-%d") +" • "+ max_date.strftime("%Y-%m-%d")

        """ВЫЧИСЛЕНИЯ ДЛЯ ПРОШЛОГО ДНЯ"""
        for i in TY_LIST:
            if TY_GROP == "y":
                time.sleep(30)
            """Выручка"""
            print("начало")
            # Выручка за прошлый день
            df_day_sales_f = df.loc[(df["Менеджер"] == i) & filter_date_day]["Выручка"].sum()
            df_day_sales = '{:,.0f}'.format(df_day_sales_f).replace(',', ' ')
            """Списания показатель"""
            # Списания за прошлый день
            df_day_sp_f = df.loc[(df["Менеджер"] == i) &filter_date_day & (df["операции списания"] != "Хозяйственные товары")]["СписРуб"].sum()
            df_day_sp = '{:,.0f}'.format(df_day_sp_f).replace(',', ' ')
            # % Списания за прошлый день
            df_day_prosent_f =  df_day_sp_f /  df_day_sales_f
            df_day_prosent = '{:,.1%}'.format(df_day_prosent_f).replace(',', ' ')
            # у словия
            sig_day_sp = "  • "
            if df_day_prosent_f >= 0.025:
                sig_day_sp = "   ❗"

            # Списания ПОТЕРИ ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_POTERY_f = df.loc[(df["Менеджер"] == i) & filter_date_day & (df["операции списания"] == "ПОТЕРИ")]["СписРуб"].sum()
            df_day_sp_POTERY = '{:,.0f}'.format(df_day_sp_POTERY_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_POTERY_prosent = df_day_sp_POTERY_f / df_day_sales_f
            df_day_sp_POTERY_prosent = '{:,.1%}'.format(df_day_sp_POTERY_prosent).replace(',', ' ')

            # Списания ХОЗЫ ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_HOZ_f = df.loc[(df["Менеджер"] == i) & filter_date_day & (df["операции списания"] == "Хозяйственные товары")]["СписРуб"].sum()
            df_day_sp_HOZ = '{:,.0f}'.format(df_day_sp_HOZ_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_HOZ_prosent = df_day_sp_HOZ_f / df_day_sales_f
            df_day_sp_HOZ_prosent = '{:,.1%}'.format(df_day_sp_HOZ_prosent).replace(',', ' ')

            # Списания Дегустации ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_DEG_f = df.loc[(df["Менеджер"] == i) & filter_date_day & (df["операции списания"] == "Дегустации")]["СписРуб"].sum()
            df_day_sp_DEG = '{:,.0f}'.format(df_day_sp_DEG_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_DEG_prosent = df_day_sp_DEG_f / df_day_sales_f
            df_day_sp_DEG_prosent = '{:,.2%}'.format(df_day_sp_DEG_prosent).replace(',', ' ')

            # Списания ОСТАЛЬНОЕ ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_PROCH_f = df.loc[(df["Менеджер"] == i) &
                                     filter_date_day &
                                     (df["операции списания"] != "Дегустации") &
                                     (df["операции списания"] != "Хозяйственные товары") &
                                     (df["операции списания"] != "ПОТЕРИ")]["СписРуб"].sum()
            df_day_sp_PROCH = '{:,.0f}'.format(df_day_sp_PROCH_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_day_sp_PROCH_prosent = df_day_sp_PROCH_f / df_day_sales_f
            df_day_sp_PROCH_prosent = '{:,.2%}'.format(df_day_sp_PROCH_prosent).replace(',', ' ')
            # CРЕДНИЙ ЧЕК

            """ВЫЧСЛЕНИЯ ДЛЯ МЕСЯЦА"""
            filter_date_mounth = (df["год"] == max_date_year ) & (df["месяц"] == max_date_mounth)
            # Выручка текущий месяц
            df_month_sales_f = df.loc[(df["Менеджер"] == i) & filter_date_mounth ]["Выручка"].sum()
            df_month_sales = '{:,.0f}'.format(df_month_sales_f).replace(',', ' ')
            """Списания показатель"""
            # Списания текущий месяц
            df_month_sp_f = df.loc[(df["Менеджер"] == i) & filter_date_mounth &
                                 (df["операции списания"] != "Хозяйственные товары")]["СписРуб"].sum()
            df_month_sp = '{:,.0f}'.format(df_month_sp_f).replace(',', ' ')
            sig_month_sp = "  • "
            if df_month_sp_f >= 0.025:
                sig_month_sp = "   ❗"


            # % Списания месяц
            df_month_prosent = df_month_sp_f/ df_month_sales_f
            df_month_prosent = '{:,.1%}'.format(df_month_prosent).replace(',', ' ')


            # Списания ПОТЕРИ
            df_month_sp_POTERY_f = df.loc[(df["Менеджер"] == i) & filter_date_mounth & (df["операции списания"] == "ПОТЕРИ")]["СписРуб"].sum()
            df_month_sp_POTERY = '{:,.0f}'.format(df_month_sp_POTERY_f).replace(',', ' ')
            # % Списания за прошлый день
            df_month_sp_POTERY_prosent = df_month_sp_POTERY_f / df_month_sales_f
            df_month_sp_POTERY_prosent = '{:,.1%}'.format(df_month_sp_POTERY_prosent).replace(',', ' ')
            # Списания Дегустации
            df_month_sp_DEG_f = df.loc[(df["Менеджер"] == i) & filter_date_mounth & (df["операции списания"] == "Дегустации")]["СписРуб"].sum()
            df_month_sp_DEG = '{:,.0f}'.format(df_month_sp_DEG_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_month_sp_DEG_prosent = df_month_sp_DEG_f / df_month_sales_f
            df_month_sp_DEG_prosent = '{:,.2%}'.format(df_month_sp_DEG_prosent).replace(',', ' ')

            # Списания ХОЗЫ
            df_month_sp_HOZ_f = df.loc[(df["Менеджер"] == i) & filter_date_mounth & (df["операции списания"] == "Хозяйственные товары")]["СписРуб"].sum()
            df_month_sp_HOZ = '{:,.0f}'.format(df_month_sp_HOZ_f).replace(',', ' ')
            # % Списания за месяц
            df_month_sp_HOZ_prosent = df_month_sp_HOZ_f / df_month_sales_f
            df_month_sp_HOZ_prosent = '{:,.1%}'.format(df_month_sp_HOZ_prosent).replace(',', ' ')

            # Списания ОСТАЛЬНОЕ ///добавить если макс воскресенье то брать 2 дня
            df_mounth_sp_PROCH_f = df.loc[(df["Менеджер"] == i) &
                                     filter_date_mounth &
                                     (df["операции списания"] != "Дегустации") &
                                     (df["операции списания"] != "Хозяйственные товары") &
                                     (df["операции списания"] != "ПОТЕРИ")]["СписРуб"].sum()
            df_mounth_sp_PROCH = '{:,.0f}'.format(df_mounth_sp_PROCH_f).replace(',', ' ')
            # % Списания за прошлый день ///добавить если макс воскресенье то брать 2 дня
            df_mounth_sp_PROCH_prosent = df_mounth_sp_PROCH_f / df_month_sales_f
            df_mounth_sp_PROCH_prosent = '{:,.2%}'.format(df_mounth_sp_PROCH_prosent).replace(',', ' ')
            # CРЕДНИЙ ЧЕК



            # region условия
            """ДЛЯ ПРОШЛОГО ДНЯ"""
            sig_day_DEG = "  • "
            if df_day_sp_DEG_f<=0:
                df_day_sp_DEG = "Дегустаций не было"
                sig_day_DEG = "❗"

            """ДЛЯ МЕСЯЦА"""
            sig_month_DEG = "  • "
            if df_month_sp_DEG_f <= 0:
                df_month_sp_DEG = "Дегустаций не было"
                sig_month_DEG = "❗"

            # endregion
            # region Переименование менеджеров

            TY_LIST = i.replace('Турова  Анна Сергеевна', 'Турова А.С') \
                .replace('Баранова Лариса Викторовна', 'Баранова Л.В') \
                .replace('Геровский Иван Владимирович ', 'Геровский И.В') \
                .replace('Изотов Вадим Валентинович', 'Изотов В.В') \
                .replace('нет ТУ', 'Нет ТУ') \
                .replace('Павлова Анна Александровна', 'Павлова А.А') \
                .replace('Бедарева Наталья Геннадьевна', 'Бедарева Н.Г') \
                .replace('Сергеев Алексей Сергеевич', 'Сергеев А.С') \
                .replace('Карпова Екатерина Эдуардовна', 'Карпова Е.Э')

            # endregion
            # region Переименование месяцов.
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

            max_date_mounth_mes = MONTHS.get(max_date_mounth, 'День недели не найден')
            max_date_mounth_mes = "  • " + max_date_mounth_mes + "  • " + str(max_date_year) + 'г'
            # endregion

            SVODKA = f'<b>👨‍💼 {TY_LIST}:</b>\n\n' \
                     f'<b>{podpis_mes}</b>\n' \
                     f'<i>{date_day}</i>\n\n' \
                     f'💰 Выручка: {df_day_sales}\n' \
                     f'💸 Списания(показатель):\n{sig_day_sp}{df_day_sp} ({df_day_prosent})\n' \
                     f'🔬 Детализация списания:\n' \
                     f'     <i>• Потери: {df_day_sp_POTERY} ({df_day_sp_POTERY_prosent})</i>\n' \
                     f'     <i>• Хозы: {df_day_sp_HOZ} ({df_day_sp_HOZ_prosent})</i>\n' \
                     f'   <i>{sig_day_DEG}Дегустации: {df_day_sp_DEG} ({df_day_sp_DEG_prosent})</i>\n' \
                     f'     <i>• Прочее: {df_day_sp_PROCH} ({df_day_sp_PROCH_prosent})</i>\n' \
                     f'🧾 Средний чек: -----\n\n' \
                     f'<b>Текущий месяц:</b>\n' \
                     f'<i>{max_date_mounth_mes}</i>\n\n' \
                     f'💰 Выручка: {df_month_sales}\n' \
                     f'💸 Списания(показатель):\n{sig_month_sp}{df_month_sp} ({df_month_prosent})\n' \
                     f'🔬 Детализация списания:\n' \
                     f'     <i>• Потери: {df_month_sp_POTERY} ({df_month_sp_POTERY_prosent})</i>\n' \
                     f'     <i>• Хозы: {df_month_sp_HOZ} ({df_month_sp_HOZ_prosent})</i>\n' \
                     f'   <i>{sig_month_DEG}Дегустации: {df_month_sp_DEG} ({df_month_sp_DEG_prosent})</i>\n' \
                     f'     <i>• Прочее: {df_mounth_sp_PROCH} ({df_mounth_sp_PROCH_prosent})</i>\n'


            BOT().bot_mes_html(mes=SVODKA)

            del df_day_sales
            del df_day_sp
            del df_month_sp_f
            del df_month_sales_f

        """BOT().bot_mes(mes="Здравствуйте, коллеги!"\
                        f"Для того, чтобы избежать возможных вопросов, я хотел бы уточнить некоторые важные моменты."\
                        f"Для учета проведенных дегустаций я выделяю отдельную строку, так как они должны проводиться регулярно, хотя бывают дни, когда их не проводят в некоторых магазинах."\
                        f"Потери - это все, что относится к продукту, включая списания, кражи, маркетинг, подарки покупателям и расходы на хозяйственные нужды."\
                        f"Списания включают в себя все статьи затрат на питание персонала, потери, кражи, маркетинг, подарки покупателям и расходы на хозяйственные нужды."\
                        f"Я выделяю знаком ❗ списания 'потери' более 2.5% и проведенные дегустации, если они были отсутствующими."\
                        f"Надеюсь, что эти пояснения помогут вам лучше понять мои расчеты и данные. Если у вас есть какие-либо вопросы, пожалуйста, не стесняйтесь спросить.")"""
        """ ln = ("Выручка",'СписРуб')
            for e in ln:
                df[e] = (df[e].astype(str)
                               .str.replace("\xa0", "")
                               .str.replace(",", ".")
                               .fillna("0")
                               .astype("float")
                               .round(2))
            print(file)
            total_memory_usage = df.memory_usage(deep=True).sum()
            print("Использовано памяти: {:.2f} GB".format(total_memory_usage / 1e9))
            # Вычисление максимального месяца
            max_month = df['По дням'].dt.month.max()
            # Вычисление количества дней в максимальном месяце
            #df = df.loc[['По дням'] =]
            # Вычисление прошлого месяца и года
            previous_month = datetime.now().month - 1
            previous_year = datetime.now().year - 1 if previous_month == 0 else datetime.now().year
            # Выборка данных, соответствующих условию
            condition = ((df['По дням'].dt.year == previous_year) & (df['По дням'].dt.month == previous_month)) | \
                        ((df['По дням'].dt.year == datetime.now().year) & (df['По дням'].dt.month == max_month) & (df['По дням'].dt.day <= max_month_days))
            df_filtered = df[condition].compute()
            # Добавление столбцов текущего месяца, прошлого месяца и прошлого года
            df_filtered['current_month'] = datetime.now().month
            df_filtered['previous_month'] = previous_month
            df_filtered['previous_year'] = previous_year
            # Добавление данных в список dask-таблиц
            dfs.append(df_filtered)
            #dfs.append(df_filtered)
            # выводим в гигабайтах
        # Соединение всех dask-таблиц в одну
        result = dd.concat(dfs)
        df_pd =result.compute()
        total_memory_usage = df_pd.memory_usage(deep=True).sum().compute()
        print("ВСЕГО Использовано памяти: {:.2f} GB".format(total_memory_usage / 1e9))
        df_pd= df_pd.groupby(['По дням', 'Склад магазин.Наименование']).sum().reset_index()
        # Преобразование dask-таблицы в pandas-таблицу и сохранение в файл
        #result.compute().DOC().to_CSV(x=result, name="test.csv")
        print(df_pd['По дням'].min())
        print(df_pd['По дням'].maxn())"""
    """Обработка продаж формирование данных для Бота"""
    def bot_mes_html(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('test')
        analitik = keys_dict.get('analitik')
        BOT_RUK_FRS = keys_dict.get('BOT_RUK_FRS')
        TY_id = keys_dict.get('TY_id')
        print(TY_id)


        #mes = 'ТЕСТ <b>жирным</b> ТЕСТ и <a href="https://www.example.com">ссылкой</a>.'

        url = f'https://api.telegram.org/bot{token}/sendMessage'

        # TEST ####################################################
        # Параметры запроса для отправки сообщения
        data = {'chat_id': test, 'text': mes, 'parse_mode': 'HTML'}
        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=data)
        # Проверка ответа от сервера Telegram
        if response.status_code == 200:
            print('Отправлено Test')
        else:
            print(f'Ошибка при отправке Test: {response.status_code}')

        # Группа аналитик ##########################################
        if BOT_ANALITIK == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params = {'chat_id': analitik, 'text': mes, 'parse_mode': 'HTML'}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response = requests.post(url, data=params)
            # Проверка ответа от сервера Telegram
            if response.status_code == 200:
                print('Отправлено Группа аналитик')
            else:
                print(f'Ошибка при отправке Группа аналитик: {response.status_code}')

        # Группа руководители ##########################################
        if BOT_RUK == "y":
            print(mes)
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params_1 = {'chat_id': BOT_RUK_FRS, 'text': mes, 'parse_mode': 'HTML'}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response = requests.post(url, data=params_1)
            # Проверка ответа от сервера Telegram
            if response.status_code == 200:
                print('Сообщение успешно Руководители!')
            else:
                print(f'Ошибка при отправке Группа руководители: {response.status_code}')
        if TY_GROP == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params_ty = {'chat_id': TY_id, 'text': mes, 'parse_mode': 'HTML'}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response_ty = requests.post(url, data=params_ty)
            # Проверка ответа от сервера Telegram
            if response_ty.status_code == 200:
                print('Сообщение успешно Руководители!')
            else:
                print(f'Ошибка при отправке Группа руководители: {response_ty.status_code}')
    """отправка сообщений d в формате HTML"""
    def to_day(self):
        # считываем данные из файла
        PROD_SVOD = pd.read_csv(PUT + "TEMP\\" + "BOT_TEMP.csv", encoding="ANSI", sep=';', parse_dates=['дата'])
        PROD_SVOD = PROD_SVOD.rename(columns={"Выручка Итого, руб с НДС": "Выручка","СписРуб": "Списания" })

        PROD_SVOD["месяц"] = PROD_SVOD["дата"].dt.month
        max_mes = PROD_SVOD["месяц"].max()

        PROD_SVOD_prmon = PROD_SVOD.copy()

        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["месяц"] == max_mes]
        PROD_SVOD["день"] = PROD_SVOD["дата"].dt.day
        max_day = PROD_SVOD["день"].max()

        PROD_SVOD_prmon = PROD_SVOD_prmon.loc[PROD_SVOD_prmon["месяц"] == max_mes-1]
        PROD_SVOD_prmon["день"] = PROD_SVOD_prmon["дата"].dt.day
        PROD_SVOD_prmon = PROD_SVOD_prmon.loc[PROD_SVOD_prmon["день"] <= max_day]

        PROD_SVOD_prmon = PROD_SVOD_prmon.rename(columns={"Выручка": "Выручка прошлый месяц", "Списания" :"Списания прошлый месяц"})


        PROD_SVOD = pd.merge(PROD_SVOD, PROD_SVOD_prmon, on=['магазин', 'день'], how='left')
        ren_mes = {
            1: 'Январь',
            2: 'Февраль',
            3: 'Март',
            4: 'Апрель',
            5: 'Май',
            6: 'Июнь',
            7: 'Июль',
            8: 'Август',
            9: 'Сентябрь',
            10: 'Октябрь',
            11: 'Ноябрь',
            12: 'Декабрь'}
        PROD_SVOD.loc[:, 'месяц название'] = PROD_SVOD['дата_x'].dt.month.replace(ren_mes)
        PROD_SVOD = PROD_SVOD.drop(columns={"дата_x","месяц_x","дата_y","месяц_y"})
        ty  =  pd.read_excel("https://docs.google.com/spreadsheets/d/1rwsBEeK_dLdpJOAXanwtspRF21Z3kWDvruani53JpRY/export?exportFormat=xlsx")
        ty = ty[["Название 1 С (для фин реза)","Менеджер"]]
        rng, replacements = RENAME().Rread()
        for i in tqdm(range(rng), desc="ПереименованиеСписок ТУ - ", colour="#808080"): ty["Название 1 С (для фин реза)"] = \
            ty["Название 1 С (для фин реза)"].str.replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
        ty = ty.rename(columns={"Название 1 С (для фин реза)": 'магазин'})

        PROD_SVOD = pd.merge(PROD_SVOD, ty, on=['магазин'], how='left')

        """obshee = PROD_SVOD.groupby(["месяц название"], as_index=False) \
            .aggregate({"Выручка":"sum","Списания":"sum" ,"Выручка прошлый месяц":"sum","Списания прошлый месяц":"sum"}) \
            .sort_values("Выручка", ascending=False)"""

        po_ty = PROD_SVOD.groupby(["Менеджер"], as_index=False) \
            .aggregate({"Выручка":"sum","Списания":"sum" ,"Выручка прошлый месяц":"sum","Списания прошлый месяц":"sum"}) \
            .sort_values("Выручка", ascending=False)

        po_ty['Изменение выручки'] = pd.to_numeric(po_ty['Выручка']) - pd.to_numeric(po_ty['Выручка прошлый месяц'])
        po_ty['Изменение расходов'] = pd.to_numeric(po_ty['Списания']) - pd.to_numeric(po_ty['Списания прошлый месяц'])
        # Определение лучших и худших менеджеров:
        best_manager = po_ty.loc[po_ty['Изменение выручки'] == po_ty['Изменение выручки'].max()]['Менеджер'].values[0]
        worst_manager = po_ty.loc[po_ty['Изменение выручки'] == po_ty['Изменение выручки'].min()]['Менеджер'].values[0]

        best_manager_spis = po_ty.loc[po_ty['Изменение расходов'] == po_ty['Изменение расходов'].max()]['Менеджер'].values[0]
        worst_manager_spis = po_ty.loc[po_ty['Изменение расходов'] == po_ty['Изменение расходов'].min()]['Менеджер'].values[0]
        #print(po_ty)
        # Выручка Изменене к прошлому месяцу лучшего менеджера
        izm_vit_best  = po_ty.loc[po_ty['Менеджер'] == best_manager]
        izm_vit_best = izm_vit_best['Изменение выручки'].sum()
        # Списания Изменене к прошлому месяцу лучшего менеджера
        izm_spis_best = po_ty.loc[po_ty['Менеджер'] == best_manager_spis]
        izm_spis_best = izm_spis_best['Изменение расходов'].sum()

        # Выручка Изменене к прошлому месяцу худщего
        izm_vit_hyd = po_ty.loc[po_ty['Менеджер'] == worst_manager]
        izm_vit_hyd = izm_vit_hyd['Изменение выручки'].sum()
        # Списания Изменене к прошлому месяцу лучшего менеджера
        izm_spis_hyd = po_ty.loc[po_ty['Менеджер'] == worst_manager_spis]
        izm_spis_hyd = izm_spis_hyd['Изменение расходов'].sum()

        # Вывод результатов для менеджеров:
        izm_spis_hyd = format(izm_spis_hyd, ',.2f').replace(',', ' ').replace('.', ',')
        izm_spis_best = format(izm_spis_best, ',.2f').replace(',', ' ').replace('.', ',')
        izm_vit_hyd = format(izm_vit_hyd, ',.2f').replace(',', ' ').replace('.', ',')
        izm_vit_best  = format(izm_vit_best, ',.2f').replace(',', ' ').replace('.', ',')
        mes_bot = \
        ("   Менеджеры   \n"
        f"💰 Выручка\n"
        f"• Лидеры: {best_manager}\n"
        f"• Изменене к прошлому месяцу: {izm_vit_best}\n"
        f"• Чуть-чуть отстают: {worst_manager}\n"
        f"• Изменене к прошлому месяцу: {izm_vit_hyd}\n"
        f"\n"
        f"💸 Списания\n"
        f"• Лидеры: {worst_manager_spis}\n"
        f"• Изменене к прошлому месяцу: {izm_spis_hyd}\n"
        f"• Чуть-чуть отстают: {best_manager_spis}\n"
        f"• Изменене к прошлому месяцу: {izm_spis_best}\n")
        # подсчет колличества магазинов
        MAG_CUNT = pd.read_csv(PUT + "TEMP\\" + "BOT\\Уникальные магазины.csv", encoding="ANSI", sep=';')
        MAG_CUNT  = MAG_CUNT["магазин"].count()
        MAG_CUNT  =(f"🛒 Количество магазинов сегодня:  {MAG_CUNT}")

        BOT().bot_mes(mes=mes_bot)
        BOT().bot_mes(mes=MAG_CUNT)
        return mes_bot
    """ежедневное инфо"""
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
"""Бот телеграм"""
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
class BOT_raschet:
    def BOT(self):
        #########################Товар дня
        TOVAR_DEY = pd.read_excel("https://docs.google.com/spreadsheets/d/1oDZQWMkKWHP4SBjZD4GYRWjZYeH1AUjRvH2z1Ik3T1g/export?exportFormat=xlsx",)
        keys_dict = dict(zip(TOVAR_DEY.iloc[:, 0], TOVAR_DEY.iloc[:, 1]))
        N1 = keys_dict.get('n1')
        print(N1)
        t2 = keys_dict.get('test')


        ##########################
        # region ПОИСК МАКСИМАЛЬНОЙ ДАТЫ
        max_date = datetime.min  # установим начальное значение для максимальной даты

        for filename in os.listdir(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\"):
                try:
                    file_date = datetime.strptime(filename[:-5], '%d.%m.%Y')  # извлекаем дату из названия файла
                    if file_date > max_date:
                        max_date = file_date  # обновляем максимальную дату, если нужно
                except ValueError:
                    pass  # если формат даты некорректный, игнорируем файл
        # дата максимальная в формате названия файла
        date_obj = datetime.strptime(str(max_date), '%Y-%m-%d %H:%M:%S')
        file_max_date = date_obj.strftime('%d.%m.%Y')
        # endregion
        # максимальный год
        max_year = max_date.year
        # максимальный месяц
        max_mounth = max_date.month
        # максимальный день
        max_day = max_date.day

        Bot = pd.DataFrame()
        # region СЕГОДНЯШНЯЯ ДАТА
        TODEY_date_file = pd.to_datetime(file_max_date, format='%d.%m.%Y').strftime('%d.%m.%Y')
        BOT().bot_mes(mes="СЕГОДНЯШНЯЯ ДАТА:\n " + str(TODEY_date_file))
        TODEY = pd.read_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + str(TODEY_date_file) + '.xlsx', parse_dates=["Дата/Время чека"],
                           date_format='%Y-%m-%d %H:%M:%S')

        if "операции" not in TODEY.columns:
            TODEY["операции"] = 0
        if "Причина списания" not in TODEY.columns:
            TODEY["Причина списания"] = 0
        if "сумма_списания" not in TODEY.columns:
            TODEY["сумма_списания"] = 0
        TODEY["Фильтр время"] = "сегодня"
        TODEY["Месяц"] = TODEY["Дата/Время чека"].dt.month
        TODEY["Год"] = TODEY["Дата/Время чека"].dt.year
        TODEY["День"] = TODEY["Дата/Время чека"].dt.day
        TODEY = TODEY[["Дата/Время чека","!МАГАЗИН!", "Стоимость позиции",'номенклатура_1с', "Сумма скидки","операции","сумма_списания","Месяц","Год","День"]]
        TODEY["Фильтр время"] = "сегодня"
        # роисаоение форматов
        ln = ("Стоимость позиции",  "Сумма скидки","сумма_списания")
        FLOAT().float_colms(name_data=TODEY, name_col=ln, name="Текущий")
        TODEY.loc[TODEY["Стоимость позиции"]>0,"операции" ] = "Выручка"
        TODEY.loc[TODEY["Сумма скидки"] > 0, "операции"] = "Скидка"

        # группировка
        TODEY = TODEY.melt(
                id_vars=["Фильтр время","Дата/Время чека", "!МАГАЗИН!" ,"операции",'номенклатура_1с',"Месяц","Год","День"],
                var_name="Статья",
                value_name="значение").reset_index(
            drop=True)
        TODEY = TODEY.groupby(["Фильтр время","Дата/Время чека", "!МАГАЗИН!" , "операции",'номенклатура_1с',"Месяц","Год","День" ], as_index=False).agg({
            "значение": "sum"}).reset_index(
            drop=True)
        MEMORY().mem_total(x="СЕГОДНЯШНЯЯ ДАТА")
        Bot = pd.concat([Bot, TODEY ], axis=0, ).reset_index(drop=True)
        del  TODEY
        gc.collect()
        MEMORY().mem_total(x="1")
        # endregion

        # region вЧЕРАШНЯЯ ДАТА
        TODEY_Last = pd.to_datetime(file_max_date, format='%d.%m.%Y') - pd.offsets.Day(1)
        TODEY_Last = TODEY_Last.strftime('%d.%m.%Y')
        BOT().bot_mes(mes="ВЧЕРАШНЯЯ ДАТА:\n " + str(TODEY_Last))
        TODEY_Last = pd.read_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + str(TODEY_Last) + '.xlsx', parse_dates=["Дата/Время чека"],
                              date_format='%Y-%m-%d %H:%M:%S')
        print("1111\n", TODEY_Last)
        TODEY_Last = TODEY_Last[["!МАГАЗИН!","Дата/Время чека", "Стоимость позиции",'номенклатура_1с', "Сумма скидки","операции","сумма_списания"]]
        TODEY_Last["Фильтр время"] = "ВЧЕРАШНЯЯ ДАТА"
        TODEY_Last["Месяц"] = TODEY_Last["Дата/Время чека"].dt.month
        TODEY_Last["Год"] = TODEY_Last["Дата/Время чека"].dt.year
        TODEY_Last["День"] = TODEY_Last["Дата/Время чека"].dt.day
        # роисаоение форматов
        ln = ("Стоимость позиции", "сумма_списания", "Сумма скидки")
        FLOAT().float_colms(name_data=TODEY_Last, name_col=ln, name="Текущий")
        TODEY_Last.loc[TODEY_Last["Стоимость позиции"] > 0, "операции"] = "Выручка"
        TODEY_Last.loc[TODEY_Last["Сумма скидки"] > 0, "операции"] = "Скидка"
        # группировка
        TODEY_Last = TODEY_Last.melt(
            id_vars=["Фильтр время","Дата/Время чека", "!МАГАЗИН!", "операции",'номенклатура_1с',"Месяц","Год","День"],
            var_name="Статья",
            value_name="значение").reset_index(
            drop=True)
        TODEY_Last = TODEY_Last.groupby(["Фильтр время", "Дата/Время чека","!МАГАЗИН!", "операции",'номенклатура_1с',"Месяц","Год","День" ], as_index=False).agg({
            "значение": "sum"}).reset_index(
            drop=True)
        print("111sssss1\n", TODEY_Last)
        MEMORY().mem_total(x="вЧЕРАШНЯЯ ДАТА")
        # ###############################################################################################################################################
        Bot = pd.concat([Bot, TODEY_Last], axis=0, ).reset_index(drop=True)
        del TODEY_Last
        gc.collect()
        MEMORY().mem_total(x="1")
        # endregion


        # region ТЕКУШИЙ МЕСЯЦ
        # строку в объект datetime
        file_max_date_ln = pd.to_datetime(file_max_date, format='%d.%m.%Y')
        file_max_date_ln = file_max_date_ln - pd.offsets.Day(1)
        # Определяем первый день текущего месяца
        first_day_of_month = file_max_date_ln.replace(day=1)
        # список дат
        dates_of_last_month = pd.date_range(start=first_day_of_month , end=file_max_date_ln, freq='D').strftime('%d.%m.%Y').tolist()
        # Фильтруем даты по условию "меньше file_max_date"
        ln_mount_tec = [date for date in dates_of_last_month if pd.to_datetime(date, format='%d.%m.%Y')]

        BOT().bot_mes(mes="ТЕКУШИЙ МЕСЯЦ:\n " + "Мин: " + str(first_day_of_month) + "\nМин: " + str(file_max_date_ln))

        Bot_tudey = pd.DataFrame()
        for file in ln_mount_tec:
            df = pd.read_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + file + '.xlsx', parse_dates=["Дата/Время чека"],
                               date_format='%Y-%m-%d %H:%M:%S')
            print(Bot_tudey)
            df = df[["Дата/Время чека", "!МАГАЗИН!", "Стоимость позиции", "Сумма скидки", "операции", "сумма_списания",'номенклатура_1с']]
            df["Фильтр время"] = "ТЕКУШИЙ МЕСЯЦ"
            df["Месяц"] = df["Дата/Время чека"].dt.month
            df["Год"] = df["Дата/Время чека"].dt.year
            df["День"] = df["Дата/Время чека"].dt.day
            df["Дата/Время чека"] = pd.to_datetime(df["Дата/Время чека"], format='%Y-%m-%d')
            df["Day"] = df["Дата/Время чека"].dt.day
            df = df.loc[df["Day"] < max_day]
            #df = df.drop(["Дата/Время чека"], axis=1)
            # роисаоение форматов
            ln = ("Стоимость позиции", "сумма_списания", "Сумма скидки")
            FLOAT().float_colms(name_data=df, name_col=ln, name="Текущий")
            df.loc[df["Стоимость позиции"] > 0, "операции"] = "Выручка"
            df.loc[df["Сумма скидки"] > 0, "операции"] = "Скидка"
            # группировка
            df = df.melt(
                id_vars=["Фильтр время", "!МАГАЗИН!", "Дата/Время чека", "операции",'номенклатура_1с',"Месяц","Год","День"],
                var_name="Статья",
                value_name="значение").reset_index(
            drop=True)
            df = df.groupby(["Фильтр время", "!МАГАЗИН!", "Дата/Время чека", "операции",'номенклатура_1с',"Месяц","Год","День"], as_index=False).agg({
                "значение": "sum"}).reset_index(
            drop=True)
            # выполнить действия для датафрейма
            Bot_tudey = pd.concat([Bot_tudey, df], axis=0, ignore_index=True).reset_index(drop=True)

            MEMORY().mem_total(x="ТЕКУШИЙ МЕСЯЦ")
            del df
            gc.collect()
            Bot_tudey = Bot_tudey.groupby(["Фильтр время","Дата/Время чека", "!МАГАЗИН!", "операции", 'номенклатура_1с',"Месяц","Год","День"], as_index=False).agg({
                "значение": "sum"}).reset_index(
                drop=True)

        # ################################################################################
        Bot = pd.concat([Bot, Bot_tudey], axis=0, ).reset_index(drop=True)
        del Bot_tudey
        gc.collect()

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
        BOT().bot_mes(mes="ПРОШЛЫЙ МЕСЯЦ:\n " + "Мин: " + str(first_day_of_last_month) +  "\nМин: " + str(last_day_of_last_month))


        Bot_last_moth = pd.DataFrame()
        for file in ln_mount_proshl:
            df = pd.read_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + file + '.xlsx',parse_dates=["Дата/Время чека"], date_format='%Y-%m-%d %H:%M:%S' )
            df  = df[["Дата/Время чека","!МАГАЗИН!","Стоимость позиции","Сумма скидки","операции","сумма_списания",'номенклатура_1с']]
            df["Фильтр время"] = "ПРОШЛЫЙ МЕСЯЦ"
            df["Месяц"] = df["Дата/Время чека"].dt.month
            df["Год"] = df["Дата/Время чека"].dt.year
            df["День"] = df["Дата/Время чека"].dt.day
            df["Дата/Время чека"]= pd.to_datetime(df["Дата/Время чека"], format='%Y-%m-%d')
            df["Day"] = df["Дата/Время чека"].dt.day
            df = df.loc[df["Day"]< max_day]

            #df = df.drop(["Дата/Время чека"], axis=1)
            # роисаоение форматов
            ln = ("Стоимость позиции", "сумма_списания", "Сумма скидки")
            FLOAT().float_colms(name_data=df, name_col=ln, name="Текущий")
            df.loc[df["Сумма скидки"] > 0, "операции"] = "Скидка"
            df.loc[df["Стоимость позиции"] > 0, "операции"] = "Выручка"
            # группировка
            print(file)
            df = df.melt(
                id_vars=["Фильтр время","Дата/Время чека", "!МАГАЗИН!", "операции",'номенклатура_1с',"Месяц","Год","День"],
                var_name="Статья",
                value_name="значение").reset_index(
            drop=True)
            df = df .groupby(["Фильтр время","Дата/Время чека", "!МАГАЗИН!",  "операции", 'номенклатура_1с',"Месяц","Год","День"], as_index=False).agg({
                "значение": "sum"}).reset_index(
            drop=True)


            # выполнить действия для датафрейма
            #df = df.reset_index(drop=True)
            Bot_last_moth = pd.concat([Bot_last_moth, df], axis=0,).reset_index(drop=True)
            del df
            gc.collect()
            MEMORY().mem_total(x="ПРОШЛЫЙ МЕСЯЦ")
            Bot_last_moth = Bot_last_moth.groupby(["Фильтр время", "Дата/Время чека", "!МАГАЗИН!","операции", 'номенклатура_1с',"Месяц","Год","День"], as_index=False).agg({
                "значение": "sum"}).reset_index(
                drop=True)

        # ################################################################################
        Bot = pd.concat([Bot, Bot_last_moth], axis=0, ).reset_index(drop=True)
        del Bot_last_moth
        gc.collect()
        MEMORY().mem_total(x="ПРОШЛЫЙ МЕСЯЦ конец")
        # endregion

        ############################### Товар дня
        TOVAR_DAY = Bot.loc[Bot["номенклатура_1с"]==N1]
        TOVAR_DAY.to_excel(PUT + "Bot\\temp\\" + "Сводная_бот_товар_дня.xlsx", index=False)
        ###############################

        Bot = Bot.groupby(["Фильтр время", "Дата/Время чека", "!МАГАЗИН!", "операции", "Месяц", "Год", "День"],
                                              as_index=False).agg({"значение": "sum"}).reset_index(drop=True)

        # Добавление ТУ
        MEMORY().mem_total(x="3")
        ty = RENAME().TY_Spravochnik()
        Bot = Bot.merge(ty, on=["!МАГАЗИН!"], how="left").reset_index(drop=True)
        del ty,TOVAR_DAY
        gc.collect()



        MEMORY().mem_total(x="4")

        """Bot.to_csv(PUT + "Bot\\temp\\" + "Сводная_бот.csv", encoding="ANSI", sep=';',
                 index=False, decimal=',')"""

        Bot.to_excel(PUT + "Bot\\temp\\" + "Сводная_бот.xlsx", index=False)
        MEMORY().mem_total(x="Память бот")
        del Bot
        gc.collect()
    def Messege(self):
        # Формирование сообщения ежедневного
        df = pd.read_excel(PUT + "Bot\\temp\\" + "Сводная_бот.xlsx")
        print((df))







        """TY_LIST = Bot.iloc[1:, 5].unique().tolist()

        if i in TY_LIST:
            # region Переименование менеджеров
             TY_LIST = i.replace('Турова  Анна Сергеевна', 'Турова А.С') \
            .replace('Баранова Лариса Викторовна', 'Баранова Л.В') \
            .replace('Геровский Иван Владимирович ', 'Геровский И.В') \
            .replace('Изотов Вадим Валентинович', 'Изотов В.В') \
            .replace('нет ТУ', 'Нет ТУ') \
            .replace('Павлова Анна Александровна', 'Павлова А.А') \
            .replace('Бедарева Наталья Геннадьевна', 'Бедарева Н.Г') \
            .replace('Сергеев Алексей Сергеевич', 'Сергеев А.С') \
            .replace('Карпова Екатерина Эдуардовна', 'Карпова Е.Э')
    
                # endregion
    
    
    
            SVODKA = f'<b>👨‍💼 {TY_LIST}:</b>\n\n' \
                     f'<b>{podpis_mes}</b>\n' \
                     f'<i>{date_day}</i>\n\n' \
                     f'💰 Выручка: {df_day_sales}\n' \
                     f'💸 Списания(показатель):\n{sig_day_sp}{df_day_sp} ({df_day_prosent})\n' \
                     f'🔬 Детализация списания:\n' \
                     f'     <i>• Потери: {df_day_sp_POTERY} ({df_day_sp_POTERY_prosent})</i>\n' \
                     f'     <i>• Хозы: {df_day_sp_HOZ} ({df_day_sp_HOZ_prosent})</i>\n' \
                     f'   <i>{sig_day_DEG}Дегустации: {df_day_sp_DEG} ({df_day_sp_DEG_prosent})</i>\n' \
                     f'     <i>• Прочее: {df_day_sp_PROCH} ({df_day_sp_PROCH_prosent})</i>\n' \
                     f'🧾 Средний чек: -----\n\n' \
                     f'<b>Текущий месяц:</b>\n' \
                     f'<i>{max_date_mounth_mes}</i>\n\n' \
                     f'💰 Выручка: {df_month_sales}\n' \
                     f'💸 Списания(показатель):\n{sig_month_sp}{df_month_sp} ({df_month_prosent})\n' \
                     f'🔬 Детализация списания:\n' \
                     f'     <i>• Потери: {df_month_sp_POTERY} ({df_month_sp_POTERY_prosent})</i>\n' \
                     f'     <i>• Хозы: {df_month_sp_HOZ} ({df_month_sp_HOZ_prosent})</i>\n' \
                     f'   <i>{sig_month_DEG}Дегустации: {df_month_sp_DEG} ({df_month_sp_DEG_prosent})</i>\n' \
                     f'     <i>• Прочее: {df_mounth_sp_PROCH} ({df_mounth_sp_PROCH_prosent})</i>\n'
    
            BOT().bot_mes_html(mes=SVODKA)





        print(Bot)"""
        return

BOT_raschet().Messege()

"""BOT().bot_mes_RUK_FRS(mes=
                f"В Дашборд добавлена новая страница:\n"
                f"Здесь Вы можете посмотреть статистику по следующим разделам:\n"
                f"\n"
                f"- Потери\n"
                f"- Кражи\n"
                f"- Питание персонала\n"
                f"- Маркетинг\n"
                f"- Подарок покупателю (бонусы)\n"
                f"- Подарок покупателю (сервисная фишка)\n"
                f"- Хозяйственные издержки\n"
                f"\n"
                f"Все данные можно отслеживать по дням, неделям, месяцам, кварталам и годам, а также сортировать по менеджерам, городам и областям.\n"
                f"\n"
                f"Надеемся, что наша информация поможет Вам сократить списания на магазинах и увеличить прибыль!\n")"""
"""отправка сообщения в группу группы руководителе"""

"""BOT().bot_mes_analitik(mes=f"Дашборд обновлен:\n"
                  f"Добавлена новая страница:\n"
                  f"СПИСАНИЯ\n"
                  f"На новой странице можно посмотреть\n"
                  f"Списания по статьям\n"
                  f"    - Потери\n"
                  f"    - Кражи\n"
                  f"    - Питание персонала\n"
                  f"    - Маркетинг\n"
                  f"    - Подарок покупателю(бонусы)\n"
                  f"    - Подарок покупателю(Сервисная фишка)\n"
                  f"    - Хозы\n"
                  f"Все можно отслеживать по дням, неделям\n"
                  f"месяцам, кварталам и годам\n\n"
                  f"Пока что все.")"""
"""оотправка сообщения в группу аналитик"""
#BOT().bot_mes(mes="Коллеги добрый день, запуск бота планируется на завтра.")
#BOT().bot_raschet()
#BOT().bot_mes_html(mes='ТЕСТ <b>жирным</b> ТЕСТ и <a href="https://www.example.com">ссылкой</a>.')

#BOT_raschet().BOT()

