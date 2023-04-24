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
    def selenium_lEN(self):
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
            id_box.send_keys('soldatovas')
            t.sleep(2)
            pass_box = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/div[2]/div/input')
            pass_box.send_keys('JQJW64JqR')
            t.sleep(2)
            print("Вход на сайт...")
            login_button = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/button/span[1]')
            login_button.click()
            t.sleep(15)

            def back(pole):
                print("Возврат")
                i = 0
                while i < 12:
                    pole.send_keys(Keys.BACKSPACE)
                    i += 1

            try:
                menu = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'appBarLeftButton')))
            except:
                print(menu.text)
            finally:
                menu.click()

            try:
                menu_op_day_cheks = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/div[2]/div[2]/div[2]/div/div/div/div[1]/div[1]/span')))
            except:
                d = "no"
            finally:
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
                    menu_op_day_cheks.click()
            print("Ура")
            # region СПИСОК ДАТ

            start_date = date(2023, 1, 1)  # начальная дата
            end_date = date(2024, 1, 1)  # конечная дата
            delta = timedelta(days=1)  # шаг даты

            dates_list = []
            while start_date < end_date:
                # преобразование даты в строку в формате 'день.месяц.год' и добавление её в список
                dates_list.append(start_date.strftime('%d.%m.%Y'))
                start_date += delta
            spisok_d = dates_list
            spisok_d = ['23.04.2023', '24.04.2023']
            for day in spisok_d:
                new_day_1 = day + " 00:00"
                new_day_2 = day + " 23:59"
                print(new_day_1)
                print(new_day_2)
                try:
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
                    menu_data_k = WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                        (By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div/div/div[1]/div[2]/div[2]/div[1]/div/div/div[2]/div/div/div/input')))
                finally:
                    menu_data_k.clear()
                    back(menu_data_k)
                    t.sleep(2)
                    print("вводим данные")
                    menu_data_k.send_keys(new_day_2)
                    print('sleep')
                t.sleep(2)
                # endregion

                try:
                    menu_primenit = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/button[2]/span[1]')))
                finally:
                    t.sleep(2)
                    menu_primenit.click()
                t.sleep(2)
                down = ""

                try:
                    dowload = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/div[1]/div/div/button/span[1]')))
                except:
                    down = "no"
                finally:
                    if down == "no":
                        print("нет кнопки")


                    else:
                        dowload.click()

                    try:
                        dowload_all = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/ul/li[2]')))
                    finally:
                        dowload_all.click()
                        t.sleep(10)
                        x = ""

                    try:
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
                #   endregion

        if Selenium == 1:
            folder_path = r"C:\Users\lebedevvv\Downloads"  # путь до папки, которую необходимо мониторить
            partial_name = "PurchasePositions"  # подстрока, которую необходимо найти
            found_file = False

            while not found_file:
                for filename in os.listdir(folder_path):
                    if partial_name in filename and filename.endswith(".xlsx"):
                        # найден файл, удовлетворяющий условиям
                        print(f"Найден файл: {filename}")
                        found_file = True

                # Проверьте, был ли найден файл. Если нет, подождите несколько секунд и повторите попытку
                if not found_file:
                    print(f"Файл {partial_name} не найден. Ожидание...")
                    t.sleep(5)  # задержка в 5 секунд перед следующей попыткой поиска файла
        else:
            t.sleep(180)
        t.sleep(5)
        ##            driver.close()
        ##            driver.switch_to.window(driver.window_handles[0])

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
                sales_day_cehk = OPEN().selenium_day_chek(name_datafreme=sales_day, name_file=str(new_filename))
                # сохранение Сгрупированного файла чеков
                sales_day_cehk.to_excel(PUT + "Selenium_set_data\\Групировка по дням\\Чеки\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил чеков: " + str(new_filename))

                # сохранение Сгрупированного файла продаж;
                sales_day_sales = OPEN().selenium_day_sales(name_datafreme=sales_day, name_file=str(new_filename))

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
                del sales_day_VEN
                gc.collect()

                # Сохранение отдельно подарочные карты
                mask_Podarok = table['Наименование товара'].str.contains("|".join(PODAROK))
                sales_day_Podarok = table[mask_Podarok]

                sales_day_Podarok.to_excel(PUT + "Selenium_set_data\\Подарочные карты\\" + new_filename, index=False)
                del sales_day_Podarok
                gc.collect()
                # endregion

                os.remove(file)
        if Selenium_skachka == 1:
            driver.close()
            driver.quit()
    """Получение данных с сетретейла"""
    def selenium_day(self):

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
            id_box.send_keys('soldatovas')
            t.sleep(2)
            pass_box = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/div[2]/div/input')
            pass_box.send_keys('JQJW64JqR')
            t.sleep(2)
            print("Вход на сайт...")
            login_button = driver.find_element(By.XPATH, '/html/body/div/div/div/div[1]/form/div/button/span[1]')
            login_button.click()
            t.sleep(15)

            def back(pole):
                print("Возврат")
                i = 0
                while i < 12:
                    pole.send_keys(Keys.BACKSPACE)
                    i += 1

            try:
                menu = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'appBarLeftButton')))
            except:
                print(menu.text)
            finally:
                menu.click()

            try:
                menu_op_day_cheks = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/div[2]/div[2]/div[2]/div/div/div/div[1]/div[1]/span')))
            except:
                d = "no"
            finally:
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
                    menu_op_day_cheks.click()
            print("Ура")
            # region СПИСОК ДАТ

            start_date = date(2023, 1, 1)  # начальная дата
            end_date = date(2024, 1, 1)  # конечная дата
            delta = timedelta(days=1)  # шаг даты

            dates_list = []
            while start_date < end_date:
                 #преобразование даты в строку в формате 'день.месяц.год' и добавление её в список
                dates_list.append(start_date.strftime('%d.%m.%Y'))
                start_date += delta
            spisok_d = dates_list
            today= datetime.now().date()
            spisok_d = ["25.04.2023"]
            for day in spisok_d:
                new_day_1 =day + " 00:00"
                new_day_2 =day + " 23:59"
                print(new_day_1)
                print(new_day_2)
                try:
                    menu_data_n=WebDriverWait(driver,15).until(EC.presence_of_element_located((By.XPATH,'/html/body/div[1]/div/div/div[2]/div/div/div/div[1]/div[2]/div[2]/div[1]/div/div/div[1]/div/div/div/input')))
                finally:
                    print("BACK")
                    back(menu_data_n)
                    t.sleep(2)
                    print("вводим данные")
                    menu_data_n.send_keys(new_day_1)
                    print('sleep')
                t.sleep(2)

                try:
                    menu_data_k=WebDriverWait(driver,15).until(EC.presence_of_element_located((By.XPATH,'/html/body/div[1]/div/div/div[2]/div/div/div/div[1]/div[2]/div[2]/div[1]/div/div/div[2]/div/div/div/input')))
                finally:
                   menu_data_k.clear()
                   back(menu_data_k)
                   t.sleep(2)
                   print("вводим данные")
                   menu_data_k.send_keys(new_day_2)
                   print('sleep')
                t.sleep(2)
            #endregion

                try:
                    menu_primenit = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/button[2]/span[1]')))
                finally:
                    t.sleep(2)
                    menu_primenit.click()
                t.sleep(2)
                down = ""

                try:
                    dowload = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[2]/div/div/div/div[1]/div[2]/div[3]/div[1]/div/div/button/span[1]')))
                except:
                    down = "no"
                finally:
                    if down == "no":
                        print("нет кнопки")


                    else:
                        dowload.click()

                    try:
                        dowload_all = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[3]/ul/li[2]')))
                    finally:
                        dowload_all.click()
                        t.sleep(10)
                        x = ""

                    try:
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

                #   endregion
        if Selenium_skachka == 1:
            driver.close()
            driver.quit()

        if Selenium == 1:
            folder_path = r"C:\Users\lebedevvv\Downloads"  # путь до папки, которую необходимо мониторить
            partial_name = "PurchasePositions"  # подстрока, которую необходимо найти
            found_file = False

            while not found_file:
                for filename in os.listdir(folder_path):
                    if partial_name in filename and filename.endswith(".xlsx"):
                        # найден файл, удовлетворяющий условиям
                        print(f"Найден файл: {filename}")
                        found_file = True

                # Проверьте, был ли найден файл. Если нет, подождите несколько секунд и повторите попытку
                if not found_file:
                    print(f"Файл {partial_name} не найден. Ожидание...")
                    t.sleep(5)  # задержка в 5 секунд перед следующей попыткой поиска файла
        else:
            t.sleep(180)
        t.sleep(5)
        ##            driver.close()
        ##            driver.switch_to.window(driver.window_handles[0])

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
                df = df.rename(columns={"Магазин": 'ID'})
                table = df.merge(spqr[['!МАГАЗИН!','ID']], on='ID', how="left")
                del df
                table = table.loc[table["Тип"].notnull()]
                table['!МАГАЗИН!'] = table['!МАГАЗИН!'].astype("str")
                table['Наименование товара'] = table['Наименование товара'].fillna("неизвестно").astype("str")

                # ######################################################################################### Загузка названий с 1 с
                spravka_nom = pd.read_csv(PUT + "\\Справочники\\Справочник номенклатуры\\1.txt", sep="\t", skiprows=1, encoding="utf-8",
                                          names=('номенклатура_1с', "cрок_годности", "группа", "подгруппа","Штрихкод",))
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
                sales_day_cehk = OPEN().selenium_day_chek(name_datafreme = sales_day, name_file= str(new_filename))
                # сохранение Сгрупированного файла чеков
                sales_day_cehk.to_excel(PUT + "Selenium_set_data\\Групировка по дням\\Чеки\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил чеков: " + str(new_filename))

                # сохранение Сгрупированного файла продаж;
                sales_day_sales = OPEN().selenium_day_sales(name_datafreme=sales_day, name_file=str(new_filename))




                sales_day_sales.to_excel(PUT + "Selenium_set_data\\Групировка по дням\\Продажи\\" + new_filename, index=False)
                bot.BOT().bot_mes(mes="Сохранен фаил чеков: " + str(new_filename[:-5]))

                del sales_day_cehk
                del sales_day
                gc.collect()
                #region СОХРАНЕНИЕ УДАЛЕННЫХ ДАННЫХ
                # Сохранение отдельно вейдинги и микромаркеты
                mask_VEN = table["!МАГАЗИН!"].str.contains("|".join(l_mag))
                sales_day_VEN = table[mask_VEN]
                sales_day_VEN.to_excel(PUT + "Selenium_set_data\\Вейдинги и микромаркет\\" + new_filename, index=False)
                del sales_day_VEN
                gc.collect()

                # Сохранение отдельно подарочные карты
                mask_Podarok = table['Наименование товара'].str.contains("|".join(PODAROK))
                sales_day_Podarok = table[mask_Podarok]

                sales_day_Podarok.to_excel(PUT + "Selenium_set_data\\Подарочные карты\\" + new_filename, index=False)
                del sales_day_Podarok
                gc.collect()
                # endregion


                os.remove(file)

    """Получение данных с сетретейла"""
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
        print(sales_day_cehk)
        sales_day_cehk["Дата/Время чека"] = sales_day_cehk["Дата/Время чека"].astype("datetime64[ns]").dt.date
        # Формирование ID Чека
        sales_day_cehk["ID_Chek"] = sales_day_cehk["ID"].astype(int).astype(str) + sales_day_cehk["Касса"].astype(int).astype(str) + sales_day_cehk["Чек"].astype(int).astype(
            str) + sales_day_cehk["Дата/Время чека"].astype(str)
        print(sales_day_cehk)
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
        sales_day_cehk['дата'] = pd.to_datetime(sales_day_cehk['дата'], format='%Y-%m-%d')
        MEMORY().mem_total(x="Обработан - Фаил чеков: " + str(name_file))

        return sales_day_cehk

    def selenium_day_sales(self, name_datafreme, name_file):
        print("ОБРАБОТКА ПРОДАЖ")
        OPEN().selenium_day_Spisania()
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





        return sales_day_sales
    def selenium_day_Spisania(self):
        for root, dirs, files in os.walk(PUT +"Данные 1с\\удалить\\" ):
            for file in files:
                os.path.basename(file)
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-8", skiprows=7,
                                 names=("!МАГАЗИН!", "Номенклатура", "Дата/Время чека","операции", "сумма_списания", "сумма_списания_без_ндс"))
                RENAME().Rread(name_data = df, name_col= "!МАГАЗИН!", name = "Списания")
                df = df.loc[df["!МАГАЗИН!"] != "Итого"]
                df = df.loc[df["Дата/Время чека"] != "Итого"]
                l_mag = ("Микромаркет", "Экопункт", "Вендинг", "Итого")
                df["!МАГАЗИН!"]= df["!МАГАЗИН!"].fillna("Не известно")
                for w in l_mag:
                    df = df[~df["!МАГАЗИН!"].str.contains(w)]
                dates = df["Дата/Время чека"].unique()
                for date in dates:
                    day_df = df[df["Дата/Время чека"] == date]
                    file_name = os.path.join(PUT+ "Данные 1с\\Списания\\", date + ".txt")
                    day_df.to_csv(file_name, sep="\t", encoding="utf-8", decimal=".", index=False)
                    MEMORY().mem_total(x="Разбиение по дням: " + os.path.basename(file))
                del df

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
#SET_RETEIL().Set_sales()
#OPEN().Sebes_put()
"""Обработка справочника номенклатуры"""
#SPRAVKA().Nomenckaltura_obrabotka()
#RENAME().Nomenklatura_set()

OPEN().selenium_lEN()

