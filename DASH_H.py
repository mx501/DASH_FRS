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
from tabulate import tabulate
# from memory_profiler import profile
import numpy as np
import winsound
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)

# расположение данных home или work
geo = "h"
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

# region комементарии
'''обновить все данные'''
'''мини дашборд для ту'''
'''Разделить в исходников на хозы и не'''
'''аномальные снижения рост отсл добавить'''
'''Ошибки по стокам и статьям'''
'''чеков на сет'''
# endregion
# region ОБНОВЛЕНИЕ ИСТОРИИ
HISTORY = "n"
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
"""чтение файлов для замены назани магазинов и базы номенклатуры хоз оваров"""
class BOT:
    def bot_mes(self, bot_mes):
        with open(PUT + "TEMP\\data_bot.txt", "r") as file:
            for line in file:
                if "TOKEN/" in line:
                    token = line.strip().split("/")[1]
                elif "chat_id/" in line:
                    chat_id = line.strip().split("/")[1]
        url = f'https://api.telegram.org/bot{token}/sendMessage'

        # Параметры запроса для отправки сообщения
        params = {'chat_id': chat_id, 'text': bot_mes}

        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram

        if response.status_code == 200:
            print('Сообщение успешно отправлено!')
        else:
            print(f'Произошла ошибка при отправке сообщения: {response.status_code}')
    """отправка сообщений"""
"""Бот телеграм"""
#BOT().bot_mes( bot_mes= "d" )
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

    def to_TEMP(self, x, name):
        x.to_csv(PUT + "TEMP\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal='.')

    def to_exel(self, x, name):
        x.to_excel(PUT + "TEMP\\" + name, index=False)
"""функция сохранения файлов по папкам"""
class NEW:
    def STATYA(self):
        STATYA = pd.read_excel(PUT + "DATA_2\\" + "@СПРАВОЧНИК_СТАТЕЙ.xlsx",
                               sheet_name="STATYA_REDAKT")
        return STATYA
    '''справочник статей_редактируется в ексель'''
    def Dat_nalog_kanal(self):
        Dat_canal_nalg = pd.read_csv(PUT + "TEMP\\" + "Дата_канал_налог.csv",
                                     sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
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
        print("получение списка каналов и режима налога, получение макс даты")
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
                        # Для сверки итоговых значений после слияния столбца результатты до слияния
                        spisisania_do = spisisania["СписРуб"].copy()
                        df_do = df["Выручка"].copy()
                        # обьеденение таблиц списания и продаж
                        df = pd.concat([df, spisisania], axis=0)
                        # лог
                        max_sales = df['По дням'].max()
                        min_sales = df['По дням'].min()
                        # лог Для сверки итоговых значений после слияния столбца результатты после слияния
                        spisisania_ps = spisisania["СписРуб"].copy()
                        df_ps = df["Выручка"].copy()
                        # запись в файл проверчных значений
                        with open(PUT + "ERROR\\" + 'Архив тест суммы после обьеденения.txt', mode='a') as f:
                            f.write(file + '\n'  "Разница в выручке:" + str(df_ps.sum() - df_do.sum()) +
                                    "  Разница в списание:" + str(spisisania_ps.sum() - spisisania_do.sum()) + '\n' +
                                    "минимальная дата - " +  str(min_sales) + "  максимальная дата - " +  str(max_sales) )
                    df.to_csv(path_to + god + "\\" + mon + "\\" + new_name + ".txt", encoding='utf-8', sep="\t",
                              index=False)  ##  сохраняет файл
                    duration = 1000
                    freq = 440
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
    def Obnovlenie(self):
        print("ОБНОВЛЕНИЕ ПРОДАЖ........\n")
        if HISTORY == "y" :
            NEW().History()
        rng, replacements = RENAME().Rread()
        for rootdir, dirs, files in os.walk(PUT + "NEW\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    df = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['По дням'], dayfirst=True, skiprows=3, names=(
                        ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                         'Себестоимость',
                         'Выручка', 'Прибыль', 'СписРуб', 'Списания, кг']))
                    god  = df['По дням'].dt.year.max()
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
                    # открывает аналогичный года по маке файла продаж
                    file_s = PUT + "Списания\\Текущий месяц\\"
                    for files in os.listdir(file_s):
                        print(files)
                        spisisania = pd.read_csv(file_s+files, sep="\t", encoding='utf-8', skiprows=7, parse_dates=['По дням'], dayfirst=True,
                                                 names=("Склад магазин.Наименование", "Номенклатура", 'По дням', "операции списания", "СписРуб", "списруб_без_ндс"))
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
                        # Для сверки итоговых значений после слияния столбца результатты до слияния
                        spisisania_do = spisisania["СписРуб"].copy()
                        df_do = df["Выручка"].copy()
                        # обьеденение таблиц списания и продаж
                        df = pd.concat([df, spisisania], axis=0)
                        # лог
                        max_sales = df['По дням'].max()
                        min_sales = df['По дням'].min()
                        # лог Для сверки итоговых значений после слияния столбца результатты после слияния
                        spisisania_ps = spisisania["СписРуб"].copy()
                        df_ps = df["Выручка"].copy()
                        # запись в файл проверчных значений
                        with open(PUT + "ERROR\\" + 'Архив тест суммы после обьеденения.txt', mode='a') as f:
                            f.write(file + '\n'  "Разница в выручке:" + str(df_ps.sum() - df_do.sum()) +
                                    "  Разница в списание:" + str(spisisania_ps.sum() - spisisania_do.sum()) + '\n' +
                                    "минимальная дата - " + str(min_sales) + "  максимальная дата - " + str(max_sales))
                        # сохранение файла
                        df.to_csv(PUT_PROD + file, encoding='utf-8', sep="\t",
                                  index=False)  ##  сохраняет файл
                        # ДЛЯ БОТА ТЕЛЕГРАМ
                        Vrem_dat = datetime.now().strftime('%d.%m.%Y %H:%M')

                        data = {
                            'Дата обновления': [Vrem_dat],
                            'Сумма продаж': [df_ps.sum().round(2)],
                            'Сумма списаний': [spisisania_ps.sum().round(2)]
                        }
                        bot_t = pd.DataFrame(data)
                        bot_t.to_csv(PUT + "TEMP\\" + 'data_bot.csv', index=False, sep=";", encoding='ANSI')

                        # очистка памяти
                        spisisania = pd.DataFrame()
                        df = pd.DataFrame()
                        gc.enable()
        """
        rng, replacements = RENAME().Rread()
        for rootdir, dirs, files in os.walk(PUT + "NEW\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    read = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=3, names=(
                        ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                         'Себестоимость',
                         'Выручка', 'Прибыль', 'СписРуб', 'Списания, кг']))
                    for i in tqdm(range(rng), desc="Переименование тт продажи -" + file, ncols=120, colour="#F8C9CE"):
                        read['Склад магазин.Наименование'] = read['Склад магазин.Наименование'].replace(
                            replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                            regex=False)
                    read = read.loc[read['Склад магазин.Наименование'] != "Итого"]
                    read = read.reset_index(drop=True)
                    read.to_csv(PUT_PROD + "Продажи, Списания, Прибыль\\Текщий год\\" + file, encoding='utf-8',
                                sep="\t", index=False)

                if ((file.split('.')[-1]) == 'xlsx'):
                    pyt_excel = os.path.join(rootdir, file)
                    read = pd.read_excel(pyt_excel, sheet_name="Sheet1")
                    for i in tqdm(range(rng), desc="Переименование тт чеки -" + file, ncols=120, colour="#F8C9CE", ):
                        read[
                            'Магазин'] = read['Магазин'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                                 regex=False)
                    read = read.reset_index(drop=True)
                    read.to_excel(PUT_PROD + "ЧЕКИ\\2023\\" + file,
                                  index=False)
                NEW().Obnovlenie_error()
                #gc.enable()
        '''отвечает за загрузку и переименование новых данных продаж и чеков'''    
                """
    def NDS_vir(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных выручки ндс\n")
        vir_NDS = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_выручка\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    vir_NDS_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8,
                                             names=("магазин", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="Переименование тт выручка ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
                        vir_NDS_00["магазин"] = vir_NDS_00["магазин"].replace(replacements["НАЙТИ"][i],
                                                                              replacements["ЗАМЕНИТЬ"][i], regex=False)
                    date = file[0:len(file) - 4]
                    vir_NDS_00 = vir_NDS_00.loc[vir_NDS_00["магазин"] != "Итого"]
                    vir_NDS_00["дата"] = date
                    vir_NDS_00["дата"] = pd.to_datetime(vir_NDS_00["дата"], dayfirst=True)
                    vir_NDS = pd.concat([vir_NDS, vir_NDS_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            vir_NDS[r] = vir_NDS[r].str.replace(',', '.')
            vir_NDS[r] = vir_NDS[r].str.replace('\xa0', '')
            vir_NDS[r] = vir_NDS[r].astype("float")
        vir_NDS["ставка выручка ндс"] = (vir_NDS["ПРОДАЖИ БЕЗ НДС"] / vir_NDS["ПРОДАЖИ С НДС"])
        vir_NDS["ПРОВЕРКАА"] = vir_NDS["ПРОДАЖИ С НДС"] * vir_NDS["ставка выручка ндс"]
        gc.enable()
        return vir_NDS
    '''отвечает за загрузку данных для  расчета ставки выручки ндс'''
    def NDS_spisania(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных списания без хозов ндс\n")
        Spisania = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_списания_без_хозов\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    Spisania_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=7,
                                              names=("магазин", "списание_без_хозов_с_ндс", "списание_без_хозов_без_ндс"))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
                        Spisania_00["магазин"] = Spisania_00["магазин"].replace(replacements["НАЙТИ"][i],
                                                                                replacements["ЗАМЕНИТЬ"][i],
                                                                                regex=False)
                    date = file[0:len(file) - 4]
                    Spisania_00 = Spisania_00.loc[Spisania_00["магазин"] != "Итого"]
                    Spisania_00["дата"] = date
                    Spisania_00["дата"] = pd.to_datetime(Spisania_00["дата"], dayfirst=True)
                    Spisania = pd.concat([Spisania, Spisania_00], axis=0)
        Ren = ["списание_без_хозов_с_ндс", "списание_без_хозов_без_ндс"]
        for r in Ren:
            Spisania[r] = Spisania[r].str.replace(',', '.')
            Spisania[r] = Spisania[r].str.replace('\xa0', '')
            Spisania[r] = Spisania[r].astype("float")
        Spisania["ставка списание без хозов ндс"] = (Spisania["списание_без_хозов_с_ндс"] / Spisania["списание_без_хозов_без_ндс"])
        Spisania["ПРОВЕРКАА"] = Spisania["списание_без_хозов_с_ндс"] * Spisania["ставка списание без хозов ндс"]
        gc.enable()
        return Spisania
    '''отвечает за загрузку данных для  расчета ставки списания без хозов ндс'''
    def NDS_pitanie(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных питание персонала ндс\n")
        Pitanie = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_питание_персонала\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    Pitanie_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=7,
                                             names=("магазин", "питание_ндс", "2.10. Питание сотрудников "))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
                        Pitanie_00["магазин"] = Pitanie_00["магазин"].replace(replacements["НАЙТИ"][i],
                                                                              replacements["ЗАМЕНИТЬ"][i],
                                                                              regex=False)
                    date = file[0:len(file) - 4]
                    Pitanie_00 = Pitanie_00.loc[Pitanie_00["магазин"] != "Итого"]
                    Pitanie_00["дата"] = date
                    Pitanie_00["дата"] = pd.to_datetime(Pitanie_00["дата"], dayfirst=True)
                    Pitanie = pd.concat([Pitanie, Pitanie_00], axis=0)
        Ren = ["питание_ндс", "2.10. Питание сотрудников "]
        for r in Ren:
            Pitanie[r] = Pitanie[r].str.replace(',', '.')
            Pitanie[r] = Pitanie[r].str.replace('\xa0', '')
            Pitanie[r] = Pitanie[r].astype("float")
        Pitanie["питание ставка ндс"] = (Pitanie["2.10. Питание сотрудников "] / Pitanie["питание_ндс"])
        Pitanie["ПРОВЕРКАА"] = Pitanie["питание_ндс"] * Pitanie["питание ставка ндс"]
        gc.enable()
        return Pitanie
    '''отвечает за загрузку данных для  расчета ставки питание с ндс'''
    def NDS_zakup(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных закуп ндс\n")
        Zakup = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ндс_закуп\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'csv'):
                    pyt_txt = os.path.join(rootdir, file)
                    Zakup_00 = pd.read_csv(pyt_txt, sep=";", encoding='ANSI', skiprows=1,
                                           names=("магазин", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС", 'ставка закуп ндс'))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
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
        pitanie = NEW().NDS_pitanie()
        spisanie_not_hoz = NEW().NDS_spisania()
        sales = NEW().NDS_vir()
        print("формирование таблицы ставок ндс")

        # обьеденене ставок ндс
        sales = sales.drop(['ПРОДАЖИ С НДС', 'ПРОДАЖИ БЕЗ НДС', 'ПРОВЕРКАА'], axis=1)
        NDS = sales.merge(spisanie_not_hoz[["магазин", "дата", "ставка списание без хозов ндс","списание_без_хозов_без_ндс"]],
                          on=["магазин", "дата"], how="left")
        NDS = NDS.merge(pitanie[["магазин", "дата", "питание ставка ндс","2.10. Питание сотрудников "]],
                        on=["магазин", "дата"], how="left")
        NDS["хозы ставка ндс"] = 0.80

        NDS = NDS.merge(Zakup[["магазин", "дата", 'ставка закуп ндс']],
                        on=["магазин", "дата"], how="left")

        # добавление режима налогобложения для установки ставки на упраенку 1'''
        canal_nalog_maxdate = Dat_canal_nalg["дата"].max()
        canal_nalog = Dat_canal_nalg.loc[Dat_canal_nalg['дата'] == canal_nalog_maxdate]
        NDS = NDS.merge(
            Dat_canal_nalg[["магазин", 'режим налогообложения', 'канал', 'канал на последний закрытый период']],
            on=["магазин"], how="outer")
        NDS.loc[NDS['режим налогообложения'] == "упрощенка", ['ставка выручка ндс', 'ставка списание без хозов ндс',
                                                              "питание ставка ндс", "хозы ставка ндс",
                                                              'ставка закуп ндс']] = [1, 1, 1, 1, 1]

        # тестовый
        DOC().to_TEMP(x=NDS, name="FINREZ_Nalog_Kanal_test.csv")
        print("Сохранен - FINREZ_Nalog_Kanal_test.csv")
        return NDS

    '''отвечает за обьеденение ставок nds  в одну таблицу вычисление налога для упращенки'''
'''отвечает первоначальную обработку, сохранение временных файлов для вычисления минимальной и максимальной даты,
сохраненние вреенного файла с каналати и режимом налогобложения'''
class PROGNOZ:
    def SALES_obrabotka(self):
        gc.enable()
        Dat_canal_nalg, finrez_max_month, finrez_max_data = NEW().Dat_nalog_kanal()
        PROD_SVOD = pd.DataFrame()
        print("ОБНОВЛЕНИЕ СВОДНОЙ ПРОДАЖ")
        start = PUT_PROD
        for rootdir, dirs, files in os.walk(start):
            for file in tqdm(files, desc="Склеивание данных   --  ", ncols=120,colour="#F8C9CE"):
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    PROD_SVOD_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['дата'],skiprows=1,
                                               dayfirst=True, names=("магазин","номенклатура","дата","количество_продаж",
                                                                     "вес_продаж","Закуп товара общий, руб с НДС", "Выручка Итого, руб с НДС", "Наценка Общая, руб с НДС","СписРуб","Списания, кг"))


                    PROD_SVOD_00 = PROD_SVOD_00.drop(["Списания, кг", "количество_продаж"], axis=1)
                    lg = ("Выручка Итого, руб с НДС", "Наценка Общая, руб с НДС",  "СписРуб", "Закуп товара общий, руб с НДС")
                    for e in lg:
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace("\xa0", "")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(",", ".")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].astype("float")
                        PROD_SVOD_00["магазин"] = PROD_SVOD_00["магазин"].astype(
                            "category")
                        PROD_SVOD_00['номенклатура'] = PROD_SVOD_00['номенклатура'].astype("str")
                        PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                                   "подарочная карта КМ 500 НОВАЯ",
                                   "подарочная карта КМ 1000 НОВАЯ")
                        for x in PODAROK:
                            PROD_SVOD_00 = PROD_SVOD_00.loc[PROD_SVOD_00['номенклатура'] != x]
                    PROD_SVOD = pd.concat([PROD_SVOD, PROD_SVOD_00], axis=0)
                gc.enable()
        # Создание столбцов Списания хозы и списания без хозов
        Hoz = RENAME().HOZY()
        mask = PROD_SVOD['номенклатура'].isin(Hoz)
        PROD_SVOD.loc[mask, "2.6. Хозяйственные товары"] = PROD_SVOD.loc[mask, 'СписРуб']
        PROD_SVOD.loc[mask, 'СписРуб'] = np.nan
        PROD_SVOD["2.6. Хозяйственные товары"] = PROD_SVOD["2.6. Хозяйственные товары"].astype("float")
        PROD_SVOD['СписРуб'] = PROD_SVOD['СписРуб'].astype("float")

        # region ГРУППИРОВКА ТАБЛИЦЫ(Без номенклатуры по дням)
        PROD_SVOD = PROD_SVOD.rename(columns={'СписРуб': "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"})
        PROD_SVOD = PROD_SVOD.groupby(["дата", "магазин"], as_index=False) \
            .aggregate({"Выручка Итого, руб с НДС": "sum",
                        "Наценка Общая, руб с НДС": "sum",
                        "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)": "sum",
                        "2.6. Хозяйственные товары": "sum", "Закуп товара общий, руб с НДС": "sum"}) \
            .sort_values("Выручка Итого, руб с НДС", ascending=False)
        # endregion
        # region ФИЛЬТРАЦИЯ ТАБЛИЦЫ > МАКС ДАТЫ КАЛЕНДАРЯ И выручка > 0
        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["Выручка Итого, руб с НДС"] > 0]
        PROD_SVOD["месяц"] = PROD_SVOD["дата"]
        PROD_SVOD.loc[~PROD_SVOD["месяц"].dt.is_month_start, "месяц"] = PROD_SVOD["месяц"] - MonthBegin()
        PROD_SVOD["номер месяца"] = PROD_SVOD["дата"].dt.month
        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["номер месяца"] > finrez_max_month]
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion
        # region ГРУПИРОВКА ПО МЕСЯЦАМ
        PROD_SVOD = PROD_SVOD.groupby(["месяц", "магазин"], as_index=False) \
            .aggregate(
            {"дата": "nunique", "Выручка Итого, руб с НДС": "sum",
             "Наценка Общая, руб с НДС": "sum",
             "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)": "sum", "2.6. Хозяйственные товары": "sum",
             "Закуп товара общий, руб с НДС": "sum"}) \
            .sort_values("магазин", ascending=False)

        PROD_SVOD = PROD_SVOD.rename(columns={'дата': "факт отработанных дней"})
        PROD_SVOD = PROD_SVOD.rename(columns={'месяц': 'дата'})
        # endregion
        # redion добавление ставки ндс вычисление выручки без ндс
        nds = NEW().Stavka_nds_Kanal()
        PROD_SVOD = PROD_SVOD.merge(nds, on=["дата", "магазин"], how="left")
        PROD_SVOD["Выручка Итого, руб без НДС"] = PROD_SVOD["Выручка Итого, руб с НДС"] * PROD_SVOD[
            "ставка выручка ндс"]
        PROD_SVOD["Закуп товара общий, руб без НДС"] = PROD_SVOD["Закуп товара общий, руб с НДС"] * PROD_SVOD['ставка закуп ндс']
        PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] = PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] * PROD_SVOD['ставка списание без хозов ндс']
        PROD_SVOD['2.5.2. НЕУ'] = PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] * 0.15
        PROD_SVOD["2.6. Хозяйственные товары"] = PROD_SVOD["2.6. Хозяйственные товары"] * PROD_SVOD["хозы ставка ндс"]
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # исключение столбцов ля округления
        ne_col = ['дата', 'магазин', 'факт отработанных дней', 'режим налогообложения', 'канал', 'канал на последний закрытый период']
        okrugl = [col for col in PROD_SVOD.columns if col not in ne_col]
        # округление
        PROD_SVOD[okrugl] = PROD_SVOD[okrugl].round(2)
        # endregion
        DOC().to_TEMP(x=PROD_SVOD, name="Временный файл_продаж.csv")
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
#NEW().Obnovlenie()
#PROGNOZ().SALES_obrabotka()
#PROGNOZ().Sales_prognoz()