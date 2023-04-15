"""Для получения информации о памяти можно использовать метод virtual_memory:

scss
Copy code
import psutil

mem = psutil.virtual_memory()
total_memory = mem.total / (1024 ** 3)
available_memory = mem.available / (1024 ** 3)
print("Всего памяти: {:.2f} GB, доступно: {:.2f} GB".format(total_memory, avail"""


import pandas as pd

# Загрузка данных в DataFrame
df = pd.read_csv('sales_data.csv')

# Использование функции pivot_table() для переворота таблицы
new_df = pd.pivot_table(df, values='Продажи', index='Месяц', columns='Магазин')