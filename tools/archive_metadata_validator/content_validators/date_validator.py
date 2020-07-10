from datetime import date
from collections import namedtuple
from time import strptime, struct_time
from typing import List, Optional, Union

from messages import ValidatorMessage, ErrorMessage, WarningMessage, ObjectLocation, StringLocation
from .content_validator import ContentValidator
from error_processor import InvalidYear


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
            _date = self._parse_date(date_str)
            if _date is None:
                return DateInfo(_date, True, date_str)
            return DateInfo(_date, False, date_str)
        return None

    @staticmethod
    def _is_future(_date):
        return _date and _date.date > date.today().timetuple()

    @staticmethod
    def _is_future_year(year: int):
        return year and year > date.today().year

    def _check_publication_years(self, spec: dict, start_date: DateInfo, end_date: DateInfo):
        messages = []
        for dotted_name, publication in self.publications(spec):
            year = self.value_at("year", publication)
            year_path = (dotted_name + ".year").split(".")
            try:
                year = int(year)
            except ValueError:
                messages.append(InvalidYear(year, year_path))
                continue
            if self._is_future_year(year):
                messages.append(
                    WarningMessage(
                        f"publication.year {year} is in the future", location))
            if year:
                if start_date and start_date.date and start_date.date.tm_year > year:
                    messages.append(
                        WarningMessage(
                            f"publication.year {year} is before study.start_date", location))

                if end_date and end_date.date and end_date.date.tm_year < year:
                    messages.append(
                        WarningMessage(
                            f"publication.year {year} is is after study.end_date", location))
        return messages

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        messages = []
        dates = []
        for context in ("study.start_date", "study.end_date"):
            date_info = self._check_optional_date(context, spec)
            if date_info and date_info.is_faulty is True:
                messages.append(
                    ErrorMessage(
                        f"date invalid ({date_info.representation})", ObjectLocation(self.file_name, context)))
                dates.append(None)
            else:
                dates.append(date_info)
        start_date, end_date = dates

        if start_date is not None and end_date is not None:
            if start_date.date > end_date.date:
                messages.append(
                    ErrorMessage(
                        f"study.end_date ({end_date.representation}) is earlier "
                        f"than study.start_date ({start_date.representation})",
                        ObjectLocation(self.file_name, "study")))

        if start_date is None and end_date is not None:
            messages.append(
                ErrorMessage(
                    f"study.end_date given, but no valid study.start_date is given",
                    ObjectLocation(self.file_name, "study")))

        if self._is_future(start_date):
            messages.append(
                ErrorMessage(
                    f"study.start_date ({start_date.representation}) is in the future",
                    ObjectLocation(self.file_name, "study")))

        if self._is_future(end_date):
            messages.append(
                WarningMessage(
                    f"study.end_date ({end_date.representation}) is in the future",
                    ObjectLocation(self.file_name, "study")))

        messages += self._check_publication_years(spec, start_date, end_date)
        return messages
