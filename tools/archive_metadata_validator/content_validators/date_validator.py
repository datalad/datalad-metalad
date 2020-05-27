from typing import List, Optional

from datetime import datetime
from dateutil import parser as date_parser

from .content_validator import ContentValidator


class DateValidator(ContentValidator):
    @staticmethod
    def _parse_date(date_string: str) -> Optional[datetime]:
        try:
            return date_parser.parse(date_string)
        except ValueError:
            return None

    def _check_optional_date(self, dotted_name, spec):
        date_str = self.value_at(dotted_name, spec)
        if date_str:
            date = self._parse_date(date_str)
            if date is None:
                return date, True
            return date, False
        return None, False

    def perform_validation(self, spec: dict) -> List:
        errors = []

        start_date, is_faulty = self._check_optional_date("study.start_date", spec)
        if is_faulty:
            errors.append("Date error: study.start_date is invalid")
        end_date, is_faulty = self._check_optional_date("study.end_date", spec)
        if is_faulty:
            errors.append("Date error: study.start_date is invalid")
        if start_date and end_date and start_date > end_date:
            errors.append("Date error: study.end_date is earlier than study.start_date")

        for publication_spec in self.value_at("study.publications", spec, default=[]):
            _, is_faulty = self._check_optional_date("publication.date", publication_spec)
            if is_faulty:
                errors.append(
                    f"Date error: faulty date in study with title: "
                    f"{publication_spec['publication']['title']}"
                )
        return errors
