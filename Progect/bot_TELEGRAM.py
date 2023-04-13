from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time,date
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
BOT_RUK_FRS = "n"

# region расположение данных home или work
geo = "w"
if geo == "h":
    # основной каталог расположение данных дашборда
    PUT = "D:\\Python\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\Текущий год\\"
    PUT_BOT = PUT + "ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\"
else:
    # основной каталог расположение данных дашборда
    PUT = "C:\\Users\\lebedevvv\\Desktop\\Dashboard\\"
    # путь до файлов с данными о продажах
    PUT_PROD = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текущий год\\"
    PUT_CHEK = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\ЧЕКИ\\2023\\"
    PUT_BOT = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\"
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
"""Переименовать магазины"""
class DOC:

    def to_CSV(self, x, name):
        x.to_csv(PUT + "TEMP\\BOT\\data\\" + name, encoding="utf-8", sep=';',
                 index=False, decimal='.')
class BOT:
    def bot_mes(self, mes):
        # получение ключей
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        token = keys_dict.get('token')
        test = keys_dict.get('test')
        analitik = keys_dict.get('analitik')
        BOT_RUK_FRS = keys_dict.get('BOT_RUK_FRS')

        # TEST ####################################################
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        # Параметры запроса для отправки сообщения
        params = {'chat_id': test, 'text':mes}
        # Отправка запроса на сервер Telegram для отправки сообщения
        response = requests.post(url, data=params)
        # Проверка ответа от сервера Telegram
        if response.status_code == 200:
            print('Отправлено Test')
        else:
            print(f'Ошибка при отправке Test: {response.status_code}')

        # Группа аналитик ##########################################
        if BOT_ANALITIK == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params = {'chat_id': analitik, 'text': mes}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response = requests.post(url, data=params)
            # Проверка ответа от сервера Telegram
            if response.status_code == 200: print('Отправлено Группа аналитик')
            else: print(f'Ошибка при отправке Группа аналитик: {response.status_code}')

        # Группа руководители ##########################################
        if BOT_RUK_FRS == "y":
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            # Параметры запроса для отправки сообщения
            params = {'chat_id': BOT_RUK_FRS, 'text': mes}
            # Отправка запроса на сервер Telegram для отправки сообщения
            response = requests.post(url, data=params)
            # Проверка ответа от сервера Telegram
            if response.status_code == 200:
                print('Сообщение успешно отправлено!')
            else:
                print(f'Ошибка при отправке Группа руководители: {response.status_code}')
    """отправка сообщений"""
    def bot_raschet(self):
        # region Сбор данных
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

        total_memory_usage = df_bot_1.memory_usage(deep=True).sum()
        print("Использовано памяти: {:.2f} GB".format(total_memory_usage / 1e9))
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

        total_memory_usage = df_bot_1.memory_usage(deep=True).sum()
        print("Использовано памяти: {:.2f} GB".format(total_memory_usage / 1e9))
        # endregion

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
        return mes_bot
    """ежедневное инфо"""
"""Бот телеграм"""
class OPENAI:
    def open_ai(self):
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
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
            max_tokens=500,
            temperature = 0.5)
        # Получение отформатированного текста
        formatted_text = response.choices[0].text.strip()

        # Вывод отформатированного текста
        BOT().bot_mes(mes=formatted_text)
        print(formatted_text)


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


BOT().bot_raschet()