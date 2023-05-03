import os
import pandas as pd
import xlsxwriter
import gc
import numpy as np
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()

PUT = "C:\\Users\\lebedevvv\\Desktop\\ДЛя жени\\Данные\\"
class RENAME:
    def Rread(self):
        print("Загрузка справочника магазинов...")
        replacements = pd.read_excel("https://docs.google.com/spreadsheets/d/1SfuC2zKUFt6PQOYhB8EEivRjy4Dz-o4WDL-IR7CT3Eg/export?exportFormat=xlsx")
        """replacements = pd.read_excel(PUT + "Справочники\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")"""
        unique_vals = set(replacements ['НАЙТИ'].unique())
        return unique_vals
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
class Obrabotka:
    def poisk(self):
        # Поиск файлов текущих продаж. Список всех файлов в папке и подпапках
        all_files = []
        for root, dirs, files in os.walk(PUT):
            for file in files:
                all_files.append(os.path.join(root, file))