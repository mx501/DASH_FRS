from datetime import datetime, timedelta, time,date
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
#import bot_TELEGRAM as bot
import winsound
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
gc.enable()
"""

date = datetime.now() - pd.Timedelta(days=1)
date =
date2 = datetime.now().strftime('%d.%m.%Y')[:-1]
print(date2)"""
PUT = "C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\"
folder_path = PUT + "Selenium_set_data\\Групировка по дням\\Чеки\\"  # замените на путь к папке, где хранятся ваши файлы


all_files = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        all_files.append(os.path.join(root, file))
# Перебираем каждый файл
for file in all_files:
        file_path = file
        print(file_path)
        # Читаем файл в pandas DataFrame
        df = pd.read_excel(file_path)
        print(os.path.basename(file)[:-5])
        # Добавляем новый столбец с именем файла
        df['filename'] =  os.path.basename(file)[:-5]
        df = df.drop(["дата"], axis=1)
        df = df.rename(columns={'filename':"дата"})
        df["дата"] = pd.to_datetime(df["дата"], format='%d.%m.%Y')
        print(df)

        # Сохраняем измененный DataFrame в тот же файл

        df.to_excel(file_path, index=False)

