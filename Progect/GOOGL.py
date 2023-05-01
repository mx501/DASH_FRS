import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
import numpy as np
import pandas
from datetime import datetime
import google.auth
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.discovery import build
"""servise-ack@my-project-51262.iam.gserviceaccount.com"""
"""
gc = gspread.service_account('C:\\Users\\lebedevvv\\Desktop\\PYTHON PROJECT\\DASH_FRS\\Progect\\client_secret.json')


df = pd.read_excel("C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Bot\\temp\\Сводная_бот_товар_дня.xlsx")
print (df)



sh = gc.open("Бот ежедневное")
worksheet = sh.worksheet("Sheet1")
worksheet.update([df.columns.values.tolist()] + df.values.tolist())


dataframe = pd.DataFrame(worksheet.get_all_records())

#

#sh = gc.create("Бот ежедневное")
#sh.share('erterwertwertwert@gmail.com',  perm_type='user', role='writer')


#values_list = worksheet.col_values(1)
# create a spreadsheet in a folder (by id)
#sht2 = gc.create("Тестовая", folder_name="Ежедневное обновление по ТУ")
print(sh)
"""


# авторизациz
creds = service_account.Credentials.from_service_account_file('C:\\Users\\lebedevvv\\Desktop\\PYTHON PROJECT\\DASH_FRS\\Progect\\client_secret.json')
service = build('sheets', 'v4', credentials=creds)



class tbl:
    def tbl_id(self, name):
        tbl().Info()
        # получение ключей

        dat = pd.read_excel('C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Bot\\temp\\Таблицы_Googl.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        tbl_id = keys_dict.get(name)

        return tbl_id
    def record(self, name,name_df ):
            tbl_id = tbl().tbl_id(name=name)
            #print("ID Таблицы: " + name[:-13] + " - " + tbl_id)
            # Имя листа, на который нужно записать данные
            sheet_name = name
            # Диапазон
            range_ = 'A1'
            del_dat = "!A1:z"
            # Очистка данных в листе перед записью
            result = service.spreadsheets().values().clear(
                spreadsheetId=tbl_id, range=del_dat).execute()
            # аголовки добавить
            zagolovok = list(name_df.columns.values)
            # Преобразование в массив
            values = [zagolovok] + name_df.values.tolist()

            # Запись данных в таблицу
            body = {'values': values}
            result = service.spreadsheets().values().update(spreadsheetId=tbl_id, range=range_, valueInputOption='RAW', body=body).execute()
            # ссылка
            Goole_url = f'https://docs.google.com/spreadsheets/d/{tbl_id}'
            print(f'Ссылка на таблицу - {name[:-13] + " - "} :  {Goole_url}')

            return Goole_url
    def stil(self,tbl_id, name):
        def size_col(tbl_id,name ):
            # ID таблицы и название листа
            spreadsheet_id = tbl_id
            sheet_name = name
            # Задание параметров для изменения ширины ячеек
            requests = [
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets'][0]['properties']['sheetId'],
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': 1
                        },
                        'properties': {
                            'pixelSize': 200
                        },
                        'fields': 'pixelSize'
                    }
                }
            ]

            # Выполнение запроса на изменение ширины ячеек
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()
        def zagolov_color(id, sheet_name):
            # Цвет фона
            background_color = {
                "red": 0.2,
                "green": 0.2,
                "blue": 0.2
            }

            # Цвет текста
            text_color = {
                "red": 1.0,
                "green": 1.0,
                "blue": 1.0
            }

            # Выполнение операций для установки цвета фона и цвета текста заголовка
            requests = [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": id,
                            "startRowIndex": 0,
                            "endRowIndex": 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": background_color
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": id,
                            "startRowIndex": 0,
                            "endRowIndex": 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "foregroundColor": text_color
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.foregroundColor"
                    }
                }
            ]

            # Отправка запроса batchUpdate
            batch_update_request = {
                "requests": requests
            }
            service.spreadsheets().batchUpdate(spreadsheetId=id, body=batch_update_request).execute()

        #zagolov_color(tbl_id, name)
        #size_col(tbl_id, name)

    def new(self):
        Ln_New = ['Турова А.С', 'Баранова Л.В', 'Геровский И.В', 'Изотов В.В', 'Томск', 'Павлова А.А', 'Бедарева Н.Г', 'Сергеев А.С', 'Карпова Е.Э']
        for i in Ln_New:
            # Создание новой таблицы
            spreadsheet = {
                'properties': {
                    'title': i +"_Текущий месяц",
                    'locale': 'ru_RU'
                },
                'sheets': [
                    {
                        'properties': {
                            'title': i +"_Текущий месяц",
                            'gridProperties': {
                                'rowCount': 300,
                                'columnCount': 20
                            }
                        }
                    }
                ]
            }

            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = spreadsheet['spreadsheetId']

            # Заполнение таблицы данными из файла Excel
            df = pd.read_excel("C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Bot\\temp\\1.xlsx")

            values = df.values.tolist()
            range_ = i +"_Текущий месяц" + '!A1:D' + str(len(values))
            body = {'values': values}

            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_, valueInputOption='USER_ENTERED', body=body).execute()

            print(f"{result.get('updatedCells')} ячеек обновлено.")

            # Установка общего доступа к таблице по ссылке
            file_id = spreadsheet['spreadsheetId']
            permission = {
                'type': 'anyone',
                'role': 'reader',
                'allowFileDiscovery': False
            }
            drive_service = build('drive', 'v3', credentials=creds)
            share_res = drive_service.permissions().create(
                fileId=file_id,
                body=permission,
                fields='id'
            ).execute()

            # Вывод ссылки на таблицу
            Goole_url = f'https://docs.google.com/spreadsheets/d/{file_id}'
            print(f'Ссылка на таблицу - {i} :  {Goole_url}')
        return
    def Info(self):
        # Параметры авторизации
        creds = service_account.Credentials.from_service_account_file('C:\\Users\\lebedevvv\\Desktop\\PYTHON PROJECT\\DASH_FRS\\Progect\\client_secret.json')
        service = build('drive', 'v3', credentials=creds)

        results = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet' and trashed = false").execute()
        items = results.get('files', [])
        alltbl = []
        if not items:
            print('No files found.')
        else:
            for item in items:
                alltbl.append(item['id'])
                #print(u'{0} ({1})'.format(item['name'], item['id']))
        # Создаем список списков с названием таблиц и их id
        table_info = [[item['name'], item['id']] for item in items]
        # Создаем DataFrame из списка списков
        df = pd.DataFrame(table_info, columns=['Название', 'ID'])
        df.to_excel('C:\\Users\\lebedevvv\\Desktop\\DASHBRD_SET\\Bot\\temp\\Таблицы_Googl.xlsx', index=False)
        return alltbl,service,creds
    def dele(self):
        alltbl,service,creds= tbl().Info()
        alltbl = ["15Xtm6fkYURByhP6hH7jkfYy2NujEcMS4f-Q8MBHm0mU","1uGKD1_nch8ZNjDNT_7iNz0csX_5jhbBviKT7fNgWUfY","1o0ClmIxplhZdSByNTt23fho7sT8skLRBiL4orgeWmBw",
                  "1k4exQcoZG3i8y6kqlVGNnImMsI-Vhk4xp4P9EB8pCKU"]

        for i in alltbl:
            file_id = i
            response = service.files().delete(fileId=file_id).execute()
            #print("удаление " ,i)

#tbl().dele()
#tbl().new()
#tbl().record(name="Карпова Е.Э_Прошлый день")
#tbl().stil()
#tbl().Info()

