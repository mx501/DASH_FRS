import os
import psutil
import pandas as pd
import gc

import xlsxwriter
import numpy as np
from datetime import datetime, timedelta, time,date
from tqdm.auto import tqdm

pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

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
    """сохранение файла чеков"""
    PUT_PLAN = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Планы\\"
# endregion

# ###################################################################################################################
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

    def TY_Spravochnik(self):
        ty = pd.read_excel("https://docs.google.com/spreadsheets/d/1qXyD0hr1sOzoMKvMyUBpfTXDwLkh0RwLcNLuiNbWmSM/export?exportFormat=xlsx")
        ty = ty[["!МАГАЗИН!","Менеджер"]]
        return ty

    """Справочник Территориальных управляющих"""



class plan:
    def palan(self):
        plan = pd.read_excel(PUT_PLAN + "Планы.xlsx")

        RENAME().Rread(name_data = plan, name_col= "!МАГАЗИН!" , name = "plan")

        Ln_data = {"ВыручкаЯнв":"01.01.2023",
            "ВыручкаФев":"01.02.2023",
            "ВыручкаМар":"01.03.2023",
            "ВыручкаАпр":"01.04.2023",
            "ВыручкаМай":"01.05.2023",
            "ВыручкаИюн":"01.06.2023",
            "ВыручкаИюл":"01.07.2023",
            "ВыручкаАвг":"01.08.2023",
            "ВыручкаСен":"01.09.2023",
            "ВыручкаОкт":"01.10.2023",
            "ВыручкаНоя":"01.11.2023",
            "ВыручкаДек":"01.12.2023",
            "Кол чековЯнв":"01.01.2023",
            "Кол чековФев":"01.02.2023",
            "Кол чековМар":"01.03.2023",
            "Кол чековАпр":"01.04.2023",
            "Кол чековМай":"01.05.2023",
            "Кол чековИюн":"01.06.2023",
            "Кол чековИюл":"01.07.2023",
            "Кол чековАвг":"01.08.2023",
            "Кол чековСен":"01.09.2023",
            "Кол чековОкт":"01.10.2023",
            "Кол чековНоя":"01.11.2023",
            "Кол чековДек":"01.12.2023",
            "Средний чекЯнв":"01.01.2023",
            "Средний чекФев":"01.02.2023",
            "Средний чекМар":"01.03.2023",
            "Средний чекАпр":"01.04.2023",
            "Средний чекМай":"01.05.2023",
            "Средний чекИюн":"01.06.2023",
            "Средний чекИюл":"01.07.2023",
            "Средний чекАвг":"01.08.2023",
            "Средний чекСен":"01.09.2023",
            "Средний чекОкт":"01.10.2023",
            "Средний чекНоя":"01.11.2023",
            "Средний чекДек":"01.12.2023"}
        Ln_tip = {"ВыручкаЯнв": "Выручка",
                   "ВыручкаФев": "Выручка",
                   "ВыручкаМар": "Выручка",
                   "ВыручкаАпр": "Выручка",
                   "ВыручкаМай": "Выручка",
                   "ВыручкаИюн": "Выручка",
                   "ВыручкаИюл": "Выручка",
                   "ВыручкаАвг": "Выручка",
                   "ВыручкаСен": "Выручка",
                   "ВыручкаОкт": "Выручка",
                   "ВыручкаНоя": "Выручка",
                   "ВыручкаДек": "Выручка",
                   "Кол чековЯнв": "Кол чеков",
                   "Кол чековФев": "Кол чеков",
                   "Кол чековМар": "Кол чеков",
                   "Кол чековАпр": "Кол чеков",
                   "Кол чековМай": "Кол чеков",
                   "Кол чековИюн": "Кол чеков",
                   "Кол чековИюл": "Кол чеков",
                   "Кол чековАвг": "Кол чеков",
                   "Кол чековСен": "Кол чеков",
                   "Кол чековОкт": "Кол чеков",
                   "Кол чековНоя": "Кол чеков",
                   "Кол чековДек": "Кол чеков",
                   "Средний чекЯнв": "Средний чек",
                   "Средний чекФев": "Средний чек",
                   "Средний чекМар": "Средний чек",
                   "Средний чекАпр": "Средний чек",
                   "Средний чекМай": "Средний чек",
                   "Средний чекИюн": "Средний чек",
                   "Средний чекИюл": "Средний чек",
                   "Средний чекАвг": "Средний чек",
                   "Средний чекСен": "Средний чек",
                   "Средний чекОкт": "Средний чек",
                   "Средний чекНоя": "Средний чек",
                   "Средний чекДек": "Средний чек"}

        plan = plan.melt(
            id_vars=[ "Тип ","ФРС/Франшиза", "Старые/ новые", "!МАГАЗИН!"],
            var_name="МЕСЯЦ",
            value_name="ПЛАН")
        plan["дата"] = plan["МЕСЯЦ"]
        plan["дата"] = plan["МЕСЯЦ"].map(Ln_data)
        plan["Показатель"] = plan["МЕСЯЦ"]
        plan["Показатель"] = plan["МЕСЯЦ"].map(Ln_tip)
        plan= plan.drop(["МЕСЯЦ"], axis=1)
        print(plan[:150])

        plan.to_excel(PUT_PLAN + "Планы ДЛЯ ДАШБОРДА.xlsx", index=False)


plan().palan()