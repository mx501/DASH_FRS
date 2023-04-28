from datetime import datetime

# получаем текущее время
now = datetime.now()
current_time = now.strftime("%H:%M:%S")

print(current_time)

f = "10:00:00"

if f<current_time:
    print(1)
else:
    print(2)