import time

from ..meter import Meter


sleep_time = .4

meter = Meter()

for value in (10, 9, 7, 20, 3):
    meter.set_value(value)
    time.sleep(sleep_time)

meter.display(True)
print()


meter = Meter()
for value in (14, 18, 18, 19, 18, 19, 17, 13):
    meter.set_value(value, force=True)
    time.sleep(sleep_time)

meter.display(True)
time.sleep(2)
print()

