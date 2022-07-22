import sched
import sys
import time


class Meter:
    def __init__(self, initial_value: int = 0):
        self.value = initial_value
        self.active = sys.stdout.isatty()
        self.last_display_time = 0.0
        self.last_displayed_value = None

    def _display_immediately(self, current_time: float):
        if self.value == self.last_displayed_value:
            self.last_display_time = current_time
            return

        # Build a display string for the current value
        display_string = f"[{self.value:4}]" + self.value * "#"

        # Erase remains from previous value
        if self.last_displayed_value is not None:
            if self.last_displayed_value > self.value:
                display_string += " " * (self.last_displayed_value - self.value)

        print(display_string + "\r", end='', flush=True)
        self.last_displayed_value = self.value
        self.last_display_time = current_time

    def display(self, force: bool = False):
        # Do not do anything if we are not on a tty
        if not self.active:
            return

        # Do not display values too fast unless forced
        current_time = time.time()
        if force is False and current_time - self.last_display_time < 0.015:
            return
        self._display_immediately(current_time)

    def set_value(self, value: int, force: bool = False):
        self.value = value
        self.display(force=force)
