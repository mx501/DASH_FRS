import os
import pandas as pd
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)


# Указываем путь до корневой директории с файлами
path_to = "D:\\Python\\Dashboard\\ПУТЬ ДО ФАЙЛОВ С ПРОДАЖАМИ\\"

# Создаем папку 1, если ее нет
if not os.path.exists(path_to + "1"):
    os.mkdir(path_to + "1")

# Проходим по всем файлам в корневой директории и заменяем точки на запятые в столбцах "СписРуб" и "Выручка"
for root, dirs, files in os.walk(path_to):
    for file in files:
        if file.endswith(".txt"):
            # Считываем файл в DataFrame
            df = pd.read_csv(os.path.join(root, file), sep="\t")

            # Заменяем точки на запятые в столбцах "СписРуб" и "Выручка"
            df["СписРуб"] = df["СписРуб"].str.replace(".", ",")
            df["Выручка"] = df["Выручка"].str.replace(".", ",")

            # Сохраняем файл с замененными значениями в папку 1
            god, mon, new_name = file.split("_")
            new_name = new_name.replace(".txt", "")
            df.to_csv(path_to + "1\\" + god + "\\" + mon + "\\" + new_name + ".txt", encoding='utf-8', sep="\t", index=False)




        df = BOT().to_day()
        # region API_K
        dat = pd.read_excel(PUT + 'TEMP\\id.xlsx')
        keys_dict = dict(zip(dat.iloc[:, 0], dat.iloc[:, 1]))
        openai.api_key = keys_dict.get('API')
        # endregion
        def generate_table_description(df):
            """
            Генерирует описание таблицы через OpenAI API
            """
            prompt = f"сформируй сообщенеи в телеграм со следующем содержанием. посчитай разницу в процентах по отношению к прошлому месяцу, выдели лучших и худших менеджеров, " \
                     f"учитывая что рост списания это убыток а  выручка это прибыл:\n\n{df}\n\nна руском"
            response = openai.Completion.create(
                engine="text-davinci-003",
                prompt=prompt,
                max_tokens=1024,
                n=1,
                stop=None,
                temperature=0.5,
            )

            description = response.choices[0].text.strip()
            return description