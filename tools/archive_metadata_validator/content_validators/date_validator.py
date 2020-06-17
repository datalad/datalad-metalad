from datetime import date
from collections import namedtuple
from time import strptime, struct_time
from typing import List, Optional, Union

from messages import ValidatorMessage
from .content_validator import ContentValidator


TIME_FORMATS = [
    "%d.%m.%Y",
    "%Y-%m-%d"
]


Date = Union[struct_time, struct_time]


DateInfo = namedtuple("DateInfo", ("date", "is_faulty", "representation"))


class DateValidator(ContentValidator):
    @staticmethod
    def _parse_date(date_string: str) -> Optional[Date]:
        for time_format in TIME_FORMATS:
            try:
                return strptime(date_string, time_format)
            except ValueError:
                continue
        return None

    def _check_optional_date(self, dotted_name, spec) -> Union[DateInfo, None]:
        date_str = self.value_at(dotted_name, spec)
        if date_str:
            date = self._parse_date(date_str)
            if date is None:
                return DateInfo(date, True, date_str)
            return DateInfo(date, False, date_str)
        return None

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        messages = []
        dates = []
        for context in ("study.start_date", "study.end_date"):
            date_info = self._check_optional_date(context, spec)
            if date_info and date_info.is_faulty is True:
                messages.append(ValidatorMessage(f"Date error: {context} "
                                                 f"('{date_info.representation}') "
                                                 f"is not valid"))
                dates.append(None)
            else:
                dates.append(date_info)
        start_date, end_date = dates

        if start_date is not None and end_date is not None:
            if start_date.date > end_date.date:
                messages.append(ValidatorMessage(f"Date error: study.end_date ('{end_date.representation}') "
                                                 f"is earlier than study.start_date ('{start_date.representation}')"))

        if start_date is None and end_date is not None:
            messages.append(ValidatorMessage("Date error: study.end_date given, "
                                             "but no valid study.start_date is given."))

        if start_date and start_date.date > date.today().timetuple():
            messages.append(ValidatorMessage(f"Date error: study.start_date "
                                             f"('{start_date.representation}') "
                                             f"is in the future."))

        return messages
