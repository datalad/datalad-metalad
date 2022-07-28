import time

from ..meter import Meter


sleep_time = .2

meter = Meter()

for value in (10, 9, 7, 20, 3):
    meter.set_value(value)
    time.sleep(sleep_time)

meter.display(True)
print()


meter = Meter()
for value in (14, 18, 18, 19, 0, 0, 18, 18, 18, 18, 18, 18, 0):
    meter.set_value(value, force=True)
    time.sleep(sleep_time)

meter.display(True)

print()

meter = Meter(label="Connections")
for _ in range(50):
    meter.set_value(3)
    time.sleep(.1)
meter.set_value(0)

print()
