# Селениум
import selenium
import warnings
import time as t
##import chromedriver_binary
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
# ######

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
# endregion
# region Переборка всех файлов сета или последние
OBNOVLENIE = 1
OBNOVLENIE_file_all = "y"
# endregion
Selenium_skachka = 1 # поиск
Selenium = 1 # ожидание
Histori_1_sayt_2 = 1 # сайт или история

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

    """Справочник Территориальных управляющих"""
"""Отвечает за переименование и подгрузку справочнкиов готовых"""
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
class SET:
    def Set_obrabotka(self):
        if Histori_1_sayt_2 ==1:
            if Selenium_skachka == 1:
                # region СКАЧИВАНИЕ С САЙТА
                warnings.filterwarnings('ignore')  ##отключаем warnings
                ua = UserAgent()
                options = webdriver.ChromeOptions()
                options.add_argument("user_agent=" + ua.random)
                driver = webdriver.Chrome(chrome_options=options)
                url = 'http://10.32.2.51:8443/operday/checks'
                driver.get(url)
                t.sleep(5)
                driver.set_window_size(1024, 600)
                driver.maximize_window()
                t.sleep(2)
                id_box = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/div[1]/div/input')
                t.sleep(1)
                id_box.send_keys('soldatovas')
                t.sleep(2)
                pass_box = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/div[2]/div/input')
                t.sleep(1)
                pass_box.send_keys('JQJW64JqR')
                t.sleep(2)
                print("Вход на сайт...")
                login_button = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/button/span[1]')
                t.sleep(1)
                login_button.click()
                t.sleep(15)

                def back(pole):
                    print("Возврат")
                    i = 0
                    while i < 12:
                        pole.send_keys(Keys.BACKSPACE)
                        i += 1

                try:
                    t.sleep(1)
                    menu = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'appBarLeftButton')))
                except:
                    t.sleep(1)
                    print(menu.text)
                finally:
                    t.sleep(1)
                    menu.click()
                try:
                    t.sleep(1)
                    menu_op_day_cheks = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/div[2]/div[2]/div[2]/div/div/div/div[1]/div[1]/span')))
                except:
                    t.sleep(1)
                    d = "no"
                finally:
                    t.sleep(1)
                    if d == "no":
                        print("но")
                        try:
                            menu_op_day = WebDriverWait(driver, 15).until(
                                EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[1]/div/div[2]/div[2]/div[1]/span')))
                        finally:
                            t.sleep(2)
                            menu_op_day.click()
                            print("click operday")
                        try:
                            menu_op_day_cheks = WebDriverWait(driver, 15).until(
                                EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[1]/div/div[2]/div[2]/div[2]/div/div/div/div[1]/div[1]')))
                        finally:
                            t.sleep(2)
                            menu_op_day_cheks.click()
                            print("m")
                    else:
                        t.sleep(1)
                        menu_op_day_cheks.click()
                print("Отправлен на скачивание.....")
                # region СПИСОК ДАТ
                today = datetime.now()
                yesterday = today - timedelta(days=1)
                date_vchera = yesterday.strftime('%d.%m.%Y')

                spisok_d = [datetime.now().strftime('%d.%m.%Y')]
                spisok_d.append(date_vchera)

                #spisok_d = ['23.04.2023', '24.04.2023', '25.04.2023', '26.04.2023']

                #start_date = date(2023, 3, 1)  # начальная дата
                # end_date = date(2024, 4, 26)  # конечная дата
                #delta = timedelta(days=1)  # шаг даты

                #dates_list = []
                #while start_date < end_date:
                   # # преобразование даты в строку в формате 'день.месяц.год' и добавление её в список
                   # dates_list.append(start_date.strftime('%d.%m.%Y'))
                   # start_date += delta
                #spisok_d = dates_list

                for day in spisok_d:
                    bot.BOT().bot_mes(mes="Скачивание файла :" + str(day))
                    new_day_1 = day + " 00:00"
                    t.sleep(1)
                    new_day_2 = day + " 23:59"
                    try:
                        t.sleep(1)
                        menu_data_n = WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                            (By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div/div/div[1]/div[2]/div[2]/div[1]/div/div/div[1]/div/div/div/input')))
                    finally:
                        print("BACK")
                        back(menu_data_n)
                        t.sleep(2)
                        print("вводим данные")
                        menu_data_n.send_keys(new_day_1)
                        print('sleep')
                    t.sleep(2)
                    try:
                        t.sleep(1)
                        menu_data_k = WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                            (By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div/div/div[1]/div[2]/div[2]/div[1]/div/div/div[2]/div/div/div/input')))
                    finally:
                        t.sleep(1)
                        menu_data_k.clear()
                        t.sleep(1)
                        back(menu_data_k)
                        t.sleep(2)
                        print("вводим данные")
                        menu_data_k.send_keys(new_day_2)
                        print('sleep')
                    t.sleep(2)
                    # endregion
                    try:
                        t.sleep(1)
                        menu_primenit = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/button[2]/span[1]')))
                    finally:
                        t.sleep(2)
                        menu_primenit.click()
                    t.sleep(2)
                    down = ""
                    try:
                        t.sleep(2)
                        dowload = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located(
                                (By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/div[1]/div/div/button/span[1]')))
                    except:
                        down = "no"
                    finally:
                        if down == "no":
                            print("нет кнопки")
                        else:
                            t.sleep(1)
                            dowload.click()

                        try:
                            dowload_all = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/ul/li[2]')))
                        finally:
                            t.sleep(1)
                            dowload_all.click()
                            t.sleep(10)
                            x = ""
                        try:
                            t.sleep(1)
                            dowload_yes = WebDriverWait(driver, 15).until(
                                EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/div/div[3]/button[2]/span[1]')))
                        except:
                            x = "no"
                        finally:
                            if x == "no":
                                print("но")
                            else:
                                t.sleep(3)
                                dowload_yes.click()
                                t.sleep(1)

                    if Selenium == 1:
                        folder_path = r"C:\Users\lebedevvv\Downloads"  # путь до папки, которую необходимо мониторить
                        partial_name = "PurchasePositions"  # подстрока, которую необходимо найти
                        found_file = False
                        bot.BOT().bot_mes(mes="Ожидание файла....")
                        while not found_file:
                            for filename in os.listdir(folder_path):
                                if partial_name in filename and filename.endswith(".xlsx"):
                                    # найден файл, удовлетворяющий условиям
                                    print(f"Найден файл: {filename}")
                                    found_file = True
                                    bot.BOT().bot_mes(mes="Фаил найден....")

                                # Проверьте, был ли найден файл. Если нет, подождите несколько секунд и повторите попытку
                                if not found_file:
                                    print(f"Файл {partial_name} не найден. Ожидание...")
                                    t.sleep(5)  # задержка в 5 секунд перед следующей попыткой поиска файла
                        t.sleep(60)
                        path_download = r"C:\Users\lebedevvv\Downloads"

                        print("Загрузка списка маазинов....")
                        spqr = pd.read_excel("https://docs.google.com/spreadsheets/d/1qXyD0hr1sOzoMKvMyUBpfTXDwLkh0RwLcNLuiNbWmSM/export?exportFormat=xlsx")
                        spqr = spqr[['ID', '!МАГАЗИН!']]
                        files = os.listdir(path_download)
                        print(files, " и ", path_download)
                        for f in files:
                            d = len(f)
                            file_name = f[0:17]
                            file = path_download + "\\" + f
                            if str(file_name) == "PurchasePositions":
                                df = pd.read_excel(file, skiprows=1)
                                MEMORY().mem_total(x="Фаил загружен: " + os.path.basename(file))

                                d = df['Дата/Время чека'][1]
                                new_filename = d[0:10] + ".xlsx"


                                df.to_excel(PUT + "Источники\\Set\\" + new_filename, index=False)
                                bot.BOT().bot_mes(mes="Фаил скачан: " + str(new_filename))

                                os.remove(file)
                if Selenium_skachka == 1:
                    driver.close()
                    driver.quit()
                SET().History()
                bot.BOT_raschet().BOT()

        return
    def History(self):
        spqr = pd.read_excel("https://docs.google.com/spreadsheets/d/1qXyD0hr1sOzoMKvMyUBpfTXDwLkh0RwLcNLuiNbWmSM/export?exportFormat=xlsx")
        spqr = spqr[['ID', '!МАГАЗИН!']]
        for root, dirs, files in os.walk(PUT + "Источники\\Set\\"):
            for file in files:
                os.path.basename(file)
                file_path = os.path.join(root, file)
                print(file_path)

                df  = pd.read_excel(file_path)
                #df = df.drop(["Магазин 1C"], axis=1)
                MEMORY().mem_total(x="Фаил загружен: " + os.path.basename(file_path))

                d = df['Дата/Время чека'][1]
                new_filename = d[0:10] + ".xlsx"
                df = df.rename(columns={"Магазин": 'ID'})
                table = df.merge(spqr[['!МАГАЗИН!', 'ID']], on='ID', how="left")
                del df
                table = table.loc[table["Тип"].notnull()]
                table['!МАГАЗИН!'] = table['!МАГАЗИН!'].astype("str")
                table['Наименование товара'] = table['Наименование товара'].fillna("неизвестно").astype("str")

                # ######################################################################################### Загузка названий с 1 с
                spravka_nom = pd.read_csv(PUT + "\\Справочники\\Справочник номенклатуры\\1.txt", sep="\t", skiprows=1, encoding="utf-8",
                                          names=('номенклатура_1с', "cрок_годности", "группа", "подгруппа", "Штрихкод",))
                spravka_dop = pd.read_excel(PUT + "\\Справочники\\Справочник номенклатуры\\Коректировка штрих кодов.xlsx")
                spravka_nom['номенклатура_1с'] = spravka_nom['номенклатура_1с'].fillna("неизвестно").astype("str")
                table["Штрихкод"] = table["Штрихкод"].astype("str").str.replace(".0", "")
                spravka_nom["Штрихкод"] = spravka_nom["Штрихкод"].astype("str").str.replace(".0", "")
                spravka_nom["штрихкод_1c"] = spravka_nom["Штрихкод"]
                table = table.merge(spravka_nom[['номенклатура_1с', "Штрихкод"]],
                                    on=["Штрихкод"], how="left").reset_index(drop=True)
                # ############################################################################################
                sales_day = table.copy()
                # удаление микромаркетов
                l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
                for w in l_mag:
                    sales_day = sales_day[~sales_day["!МАГАЗИН!"].str.contains(w)].reset_index(drop=True)

                # удаление подарочных карт
                PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                           "подарочная карта КМ 500 НОВАЯ",
                           "подарочная карта КМ 1000 НОВАЯ")
                for x in PODAROK:
                    sales_day = sales_day[~sales_day['Наименование товара'].str.contains(x)]
                sales_day.to_excel(PUT + "Selenium_set_data\\Tекущий день\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил общих продаж: " + str(new_filename))
                # обработка файла чеков
                sales_day_cehk = SET().selenium_day_chek(name_datafreme=sales_day, name_file=str(new_filename))
                # сохранение Сгрупированного файла чеков

                sales_day_cehk.to_excel(PUT + "Selenium_set_data\\Групировка по дням\\Чеки\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил чеков: " + str(new_filename))

                # сохранение Сгрупированного файла продаж;
                sales_day_sales = SET().selenium_day_sales(name_datafreme=sales_day, name_file=str(new_filename))
                sales_day_sales.to_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил чеков: " + str(new_filename[:-5]))

                del sales_day_cehk
                del sales_day
                gc.collect()
                # region СОХРАНЕНИЕ УДАЛЕННЫХ ДАННЫХ
                # Сохранение отдельно вейдинги и микромаркеты
                mask_VEN = table["!МАГАЗИН!"].str.contains("|".join(l_mag))
                sales_day_VEN = table[mask_VEN]
                sales_day_VEN.to_excel(PUT + "Selenium_set_data\\Вейдинги и микромаркет\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен Вейдинг: " + str(new_filename[:-5]))
                del sales_day_VEN
                gc.collect()

                # Сохранение отдельно подарочные карты
                mask_Podarok = table['Наименование товара'].str.contains("|".join(PODAROK))
                sales_day_Podarok = table[mask_Podarok]

                sales_day_Podarok.to_excel(PUT + "Selenium_set_data\\Подарочные карты\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен Подарочные карты: " + str(new_filename[:-5]))
                del sales_day_Podarok
                gc.collect()

                # Сохранение отдельно анулированные и возвращенные чеки
                sales_null = table.loc[(table["Тип"] == "Отмена") | (table["Тип"] == "Возврат")]
                sales_null.to_excel(PUT + "Selenium_set_data\\Анулированные и возврат чеки\\" + new_filename, index=False)
                if geo == "w":
                    sales_null.to_excel("P:\\Общие\\ЭБД\\Франшиза\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен Возвраты: " + str(new_filename[:-5]))
                del sales_null
                gc.collect()
                # bot.BOT_raschet().BOT()

                # endregion

        return
    def Sayt(self):

        return
    def selenium_day_chek(self, name_datafreme, name_file):
        MEMORY().mem_total(x="Формирование чеков: ")
        sales_day_cehk = name_datafreme[["Тип", "!МАГАЗИН!", "ID", "Дата/Время чека", "Касса", "Чек", "Стоимость позиции", "Код товара"]]
        sales_day_cehk = sales_day_cehk.loc[sales_day_cehk["Тип"] =="Продажа"]
        sales_day_cehk = sales_day_cehk.drop(["Тип"], axis=1)
        # время обновления
        set_check_date = sales_day_cehk["Дата/Время чека"].max()
        with open(PUT + "Дата и время обновления\DATE.txt", "w") as f:
            f.write(str(set_check_date))
        del set_check_date
        sales_day_cehk["Дата/Время чека"] = sales_day_cehk["Дата/Время чека"].astype("datetime64[ns]").dt.date
        # Формирование ID Чека
        sales_day_cehk["ID_Chek"] = sales_day_cehk["ID"].astype(int).astype(str) + sales_day_cehk["Касса"].astype(int).astype(str) + sales_day_cehk["Чек"].astype(int).astype(
            str) + sales_day_cehk["Дата/Время чека"].astype(str)
        sales_day_cehk = sales_day_cehk.drop(["Касса", "Чек"], axis=1)
        # удаление не нужных символов
        FLOAT().float_colm(name_data=sales_day_cehk, name_col="Стоимость позиции", name="set_check")
        # Групировки по дням
        sales_day_cehk = sales_day_cehk.groupby(["!МАГАЗИН!", "ID", "Дата/Время чека", "ID_Chek"], as_index=False).agg({
            "Стоимость позиции": "sum",
            "Код товара": [("Количество товаров в чеке", "count"), ("Количество уникальных товаров в чеке", "nunique")]})
        # переименовываем столбцы
        sales_day_cehk.columns = ['!МАГАЗИН!', "ID", 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке',
                             'Количество уникальных товаров в чеке']
        # выбираем нужные столбцы и сортируем по дате/времени чека в порядке убывания
        sales_day_cehk = sales_day_cehk[
            ["ID", '!МАГАЗИН!', 'Дата/Время чека', 'ID_Chek', 'Стоимость позиции', 'Количество товаров в чеке', 'Количество уникальных товаров в чеке']] \
            .sort_values('Дата/Время чека', ascending=False) \
            .reset_index(drop=True)
        # групировка по магазинам
        sales_day_cehk = sales_day_cehk.groupby(["ID", "!МАГАЗИН!", "Дата/Время чека"], as_index=False) \
            .agg({"Стоимость позиции": "sum",
                  'ID_Chek': "count",
                  "Количество товаров в чеке": "mean",
                  "Количество уникальных товаров в чеке": "mean"}) \
            .sort_values("Дата/Время чека", ascending=False).reset_index(drop=True)
        # дбавление среднего чека
        sales_day_cehk["Средний чек"] = sales_day_cehk["Стоимость позиции"] / sales_day_cehk["ID_Chek"]
        # переименование столбцов
        sales_day_cehk = sales_day_cehk.rename(columns={ "Дата/Время чека": "дата", "Стоимость позиции": "выручка",
                                              "ID_Chek": "Количество чеков", "Количество товаров в чеке": "количество товаров в чеке",
                                              "Количество уникальных товаров в чеке": "количество уникальных товаров в чеке"})
        # округление
        sales_day_cehk= sales_day_cehk.round(2)
        sales_day_cehk['filename'] = os.path.basename(name_file)[:-5]
        sales_day_cehk = sales_day_cehk.drop(['дата'], axis=1)
        sales_day_cehk = sales_day_cehk.rename(columns={'filename': 'дата'})
        sales_day_cehk["дата"] = pd.to_datetime(sales_day_cehk["дата"], format='%d.%m.%Y')

        MEMORY().mem_total(x="Обработан - Фаил чеков: " + str(name_file))

        return sales_day_cehk
    """ОБРАБОТКА ЧЕКОВ"""
    def selenium_day_sales(self, name_datafreme, name_file):
        bot.BOT().bot_mes(mes="Создание файла продаж... ")
        sales_day_sales = name_datafreme[["Дата/Время чека","ID","!МАГАЗИН!","Тип","Наименование товара","номенклатура_1с","Количество","Стоимость позиции","Сумма скидки"]]
        sales_day_sales = sales_day_sales.loc[sales_day_sales["Тип"] == "Продажа"]
        sales_day_sales = sales_day_sales.drop(["Тип"], axis=1)
        sales_day_sales["Дата/Время чека"] = sales_day_sales["Дата/Время чека"].astype("datetime64[ns]").dt.date
        sales_day_sales["Дата/Время чека"] = pd.to_datetime(sales_day_sales["Дата/Время чека"], format='%Y-%m-%d')
        ln = ("Стоимость позиции","Количество","Сумма скидки")
        FLOAT().float_colms(name_data = sales_day_sales , name_col = ln, name ="sales_day_sales" )
        sales_day_sales = sales_day_sales.groupby(["Дата/Время чека","ID","!МАГАЗИН!","Наименование товара","номенклатура_1с"], as_index=False) \
            .agg({"Стоимость позиции": "sum",
                  "Количество":"sum",
                  "Сумма скидки": "sum"}) \
            .sort_values("!МАГАЗИН!", ascending=False).reset_index(drop=True)
        print(sales_day_sales)

        sales_day_sales['filename'] = os.path.basename(name_file)[:-5]
        sales_day_sales = sales_day_sales.drop(["Дата/Время чека"], axis=1)
        sales_day_sales = sales_day_sales.rename(columns={'filename': "Дата/Время чека"})
        sales_day_sales["Дата/Время чека"] = pd.to_datetime(sales_day_sales["Дата/Время чека"], format='%d.%m.%Y')



        # Доавление списания
        SET().selenium_day_Spisania()
        file_spis = PUT + "Данные 1с\\Списания\\" + name_file[:-5] + ".txt"
        if os.path.exists(file_spis):
            spis = pd.read_csv(file_spis, sep="\t", skiprows=1,encoding="utf-8",
                                    names=("!МАГАЗИН!", "номенклатура_1с", "Дата/Время чека","операции","сумма_списания",  "сумма_списания_nds"))

            bot.BOT().bot_mes(mes="Найден фаил списаний...")
            spis['filename'] = os.path.basename(name_file)[:-5]
            spis = spis.drop(["Дата/Время чека"], axis=1)
            spis = spis.rename(columns={'filename': "Дата/Время чека"})
            spis["Дата/Время чека"] = pd.to_datetime(spis["Дата/Время чека"], format='%d.%m.%Y')


            sales_day_sales = pd.concat([sales_day_sales, spis], axis=0).reset_index(drop=True)



        return sales_day_sales
    """ОБРАБОТКА ПРОДАЖ"""
    def selenium_day_Spisania(self):
        for root, dirs, files in os.walk(PUT + "Данные 1с\\СПИСАНИЯ НОВЫЕ\\"):
            for file in files:
                os.path.basename(file)
                file_path = os.path.join(root, file)
                print(file_path)
                df = pd.read_csv(file_path, sep="\t", encoding='utf-8', skiprows=7, parse_dates=["Дата/Время чека"], date_format="%d.%m.%Y",
                                 names=("!МАГАЗИН!","номенклатура_1с", "Дата/Время чека","операции", "сумма_списания", "сумма_списания_НДС"))
                print( df)
                RENAME().Rread(name_data=df, name_col="!МАГАЗИН!", name="Списания")
                df = df.loc[df["!МАГАЗИН!"] != "Итого"]
                df = df.loc[df["Дата/Время чека"] != "Итого"]
                l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
                df["!МАГАЗИН!"] = df["!МАГАЗИН!"].fillna("Не известно")
                for w in l_mag:
                    df = df[~df["!МАГАЗИН!"].str.contains(w)]

                # "<Объект не найден>" и пустые удалить из столбца причина

                #df["Дата/Время чека"] = df["Дата/Время чека"].str[:10]

                dates = df["Дата/Время чека"].unique()
                date_str = dates.strftime("%d.%m.%Y")

                print(df)

                #df["Дата/Время чека"] = pd.to_datetime(df["Дата/Время чека"], format='%d.%m.%Y')

                for date in date_str :
                    print(date)
                    df["Дата/Время чека"] = pd.to_datetime(df["Дата/Время чека"], format="%d.%m.%Y")
                    day_df = df.loc[df["Дата/Время чека"] == pd.to_datetime(date, format="%d.%m.%Y")]
                    print(df)
                    file_name = os.path.join(PUT + "Данные 1с\\Списания\\", date + ".txt")
                    day_df.to_csv(file_name, sep="\t", encoding="utf-8", decimal=".", index=False)
                    MEMORY().mem_total(x="Разбиение по дням: " + os.path.basename(file))
                #os.remove(PUT + "Данные 1с\\СПИСАНИЯ НОВЫЕ\\" +file)

            gc.collect()
        bot.BOT().bot_mes(mes="Дробление файла списания.....")
        return

    """РАЗДРОБЛЕНИЕ ФАЙЛА ПИСАНИЙ НА ДНИ"""







SET().Set_obrabotka()

