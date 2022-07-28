import sys
import threading
import time

from typing import IO


hide_cursor = '\x1b[?25l'
show_cursor = '\x1b[?25h'


class Meter:

    spinner_characters = (
        " | ", " / ", " - ", " \\ ",
        " | ", " / ", " - ", " \\ "
    )

    def __init__(self,
                 initial_value: int = 0,
                 label: str = "",
                 file: IO = sys.stdout):
        self.value = initial_value
        self.label = label + " " if label else ""
        self.file = file
        self.active = self.file.isatty()
        self.last_display_time = 0.0
        self.last_displayed_value = None
        self.last_spinner_time = 0.0
        self.spinner_state = 0
        self.lock = threading.Lock()
        if self.active:
            print(hide_cursor, end="", file=self.file, flush=True)

    def __del__(self):
        if self.active:
            print(show_cursor, end="", file=self.file, flush=True)

    def _update_spinner(self,
                        current_time: float,
                        standalone: bool = False
                        ):

        # Update spinner only, if enough time has passed
        if current_time - self.last_spinner_time > 0.07:
            self.last_spinner_time = current_time
            self.spinner_state += 1
            self.spinner_state %= len(Meter.spinner_characters)
        print(
            self.label + Meter.spinner_characters[self.spinner_state],
            end="\r" if standalone is True else "",
            flush=standalone,
            file=self.file
        )

    def _display_immediately(self,
                             current_time: float
                             ):
        if self.value == self.last_displayed_value:
            self._update_spinner(current_time=current_time, standalone=True)
            self.last_display_time = current_time
            return

        # Build a display string for the current value
        display_string = f"[{self.value:4}]" + self.value * "#"

        # Erase remains from previous value
        if self.last_displayed_value is not None:
            if self.last_displayed_value > self.value:
                display_string += " " * (self.last_displayed_value - self.value)

        self._update_spinner(current_time=current_time)
        print(display_string + "\r", end='', flush=True, file=self.file)
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

    def goto(self, value: int):
        self.set_value(value)
