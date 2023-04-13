"""Для получения информации о памяти можно использовать метод virtual_memory:

scss
Copy code
import psutil

mem = psutil.virtual_memory()
total_memory = mem.total / (1024 ** 3)
available_memory = mem.available / (1024 ** 3)
print("Всего памяти: {:.2f} GB, доступно: {:.2f} GB".format(total_memory, avail"""