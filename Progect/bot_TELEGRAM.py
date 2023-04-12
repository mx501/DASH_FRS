from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time
from pandas.tseries.offsets import MonthBegin
import os
import pandas as pd
from tqdm.auto import tqdm
import openai
import sys
import math
import gc
import requests
# from memory_profiler import profile
import numpy as np
import calendar
import winsound

pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

# Отправлять ли в группу вечеринка аналитиков Сообщения?
BOT_ANALITIK = "n"
BOT_RUK_FRS = "n"

# region расположение данных home или work
geo = "w"
if geo == "h":
    # основной каталог расположение данных дашборда
    PUT = "D:\\Python\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
else:
    # основной каталог расположение данных дашборда
    PUT = "C:\\Users\\lebedevvv\\Desktop\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текущий год\\"
    PUT_CHEK = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\ЧЕКИ\\2023\\"
# endregion
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

class BOT:
    def bot_mes(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('test')
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Параметры запроса для отправки сообщения
        params = {'chat_id': test, 'text':mes}

        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram

        if response.status_code == 200:
            print('Сообщение успешно отправлено!')
        else:
            print(f'Произошла ошибка при отправке сообщения: {response.status_code}')
            """отправка сообщений"""
    def bot_mes_analitik(self, mes):
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        # Создаем словарь ключей
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        # Получаем значение по id
        token = keys_dict.get('token')
        analitik = keys_dict.get('analitik')

        url = f'https://api.telegram.org/bot{token}/sendMessage'

        # Параметры запроса для отправки сообщения
        params = {'chat_id': analitik, 'text':mes}

        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram

        if response.status_code == 200:
            print('Сообщение успешно отправлено!')
        else:
            print(f'Произошла ошибка при отправке сообщения: {response.status_code}')
    def bot_mes_RUK_FRS(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('BOT_RUK_FRS')
        print(token)
        print(test)
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Параметры запроса для отправки сообщения
        params = {'chat_id': test, 'text': mes}

        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram

        if response.status_code == 200:
            print('Сообщение успешно отправлено!')
        else:
            print(f'Произошла ошибка при отправке сообщения: {response.status_code}')
            """отправка сообщений"""
    def bot_raschet(self):
        return
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
        if BOT_ANALITIK == "y":
            BOT().bot_mes_analitik(mes=mes_bot)
            print("Отправлено в группу Аналитики - Топы")
            BOT().bot_mes_analitik(mes=MAG_CUNT)
            print("Отправлено в группу Аналитики - кол магазинов")
        if BOT_RUK_FRS == "y":
            BOT().bot_mes_RUK_FRS(mes=mes_bot)
            print("Отправлено в группу руководители ФРС - Топы")
            BOT().bot_mes_RUK_FRS(mes=MAG_CUNT)
            print("Отправлено в группу руководители ФРС - кол магазинов")

        return mes_bot
    """ежедневное инфо"""
"""Бот телеграм"""
class OPENAI:
    def open_ai(self):
        df = BOT().to_day()
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
        def generate_table_description(df):
            prompt = f"выведи количество отправленых и полученых токенов" \
                     f":\n\n{df}\n\nна руском"
            response = openai.Completion.create(
                engine="text-curie-001",
                prompt=prompt,
                max_tokens=1024,
                n=1,
                stop=None,
                temperature=0.5,)

            description = response.choices[0].text.strip()
            return description

        # Расчет разницы между текущим и прошлым месяцем:
        df['Изменение выручки'] = pd.to_numeric(df['Выручка']) - pd.to_numeric(df['Выручка прошлый месяц'])
        df['Изменение расходов'] = pd.to_numeric(df['Списания']) - pd.to_numeric(df['Списания прошлый месяц'])

        # Определение лучших и худших менеджеров:
        best_manager = df.loc[df['Изменение выручки'] == df['Изменение выручки'].max()]['Менеджер'].values[0]
        worst_manager = df.loc[df['Изменение выручки'] == df['Изменение выручки'].min()]['Менеджер'].values[0]

        # Генерация описания таблицы:
        description = response.choices[0].text

        # Вывод результатов:
        print(description)
        print('\n')
        print(f"Best Manager: {best_manager}")
        print(f"Worst Manager: {worst_manager}")
    def open_ai_curi(self):
        #mes_bot = BOT().to_day()
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
        # Определение текста запроса
        request = "Дашборд обновлен: Добавлена новая страница:СПИСАНИЯ На новой странице можно посмотреть " \
                  "Списания по статьям- Потери- Кражи- Питание персонала- Маркетинг- " \
                  "Подарок покупателю(бонусы)- Подарок покупателю(Сервисная фишка)- ХозыВсе можно отслеживать по дням, неделяммесяцам, кварталам и годам сортировать по менеджерам городам областям."
        #request = mes_bot
        # Форматирование текста
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=(f"Составь сообщение для телеграм, примени форматирование красивое строгом виде, в конце отформатируй и добавь что эта информация вам поможет сократить списания на магазинах и увеличить прибыль:\n{request}\n\n"),
            max_tokens=1000,
            temperature = 0.5)
        # Получение отформатированного текста
        formatted_text = response.choices[0].text.strip()

        # Вывод отформатированного текста
        BOT().bot_mes(mes=formatted_text)
        print(formatted_text)
