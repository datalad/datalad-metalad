import sys
import threading
import time


class Meter:

    spinner_characters = (" >.. ", " .>. ", " ..> ",
                          " ..< ", " .<. ", " <.. ",)  #("|", "/", "-", "\\", "|", "/", "-", "\\")

    def __init__(self, initial_value: int = 0):
        self.value = initial_value
        self.active = sys.stdout.isatty()
        self.last_display_time = 0.0
        self.last_displayed_value = None
        self.spinner_state = 0
        self.lock = threading.Lock()

    def _update_spinner(self, standalone: bool = False):
        self.spinner_state += 1
        self.spinner_state %= len(Meter.spinner_characters)
        print(
            Meter.spinner_characters[self.spinner_state],
            end="\r" if standalone is True else "",
            flush=standalone
        )

    def _display_immediately(self, current_time: float):
        if self.value == self.last_displayed_value:
            self._update_spinner(standalone=True)
            self.last_display_time = current_time
            return

        # Build a display string for the current value
        display_string = f"[{self.value:4}]" + self.value * "#"

        # Erase remains from previous value
        if self.last_displayed_value is not None:
            if self.last_displayed_value > self.value:
                display_string += " " * (self.last_displayed_value - self.value)

        self._update_spinner()
        print(display_string + "\r", end='', flush=True)
        self.last_displayed_value = self.value
        self.last_display_time = current_time

    def _locked_display(self, force: bool = True):
        # Do not display values too fast unless forced
        current_time = time.time()
        if not force and current_time - self.last_display_time < 0.015:
            return
        self._display_immediately(current_time)

    def display(self, force: bool = True):
        if not self.active:
            return
        with self.lock:
            self._locked_display(force=force)

    def set_value(self, value: int, force: bool = True):
        with self.lock:
            self.value = value
            self._locked_display(force=force)
