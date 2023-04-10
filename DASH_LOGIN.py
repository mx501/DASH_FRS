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