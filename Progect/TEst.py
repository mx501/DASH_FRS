from datetime import datetime

# получаем текущее время
now = datetime.now()
current_time = now.strftime("%H:%M:%S")

print(current_time)

# получаем текущее время
now = datetime.now()

# округляем до ближайшего часа
rounded_hour = (now.hour + 1) if now.minute >= 30 else (now.hour)

# создаем новое время, округленное до часа
rounded_time = datetime(now.year, now.month, now.day, rounded_hour, 0, 0)

# преобразуем в строку и выводим на экран
current_time = rounded_time.strftime("%H:%M")
print("Текущее время (округлено до часа):", current_time)

f = "10:00:00"

if f<current_time:
    print(1)
else:
    print(2)