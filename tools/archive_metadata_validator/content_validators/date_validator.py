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

        previous_date = None
        for context in ("study.start_date", "study.end_date"):
            date, is_faulty = self._check_optional_date(context, spec)
            if is_faulty:
                errors.append(f"Date error: {context} is invalid")
            if previous_date and previous_date > date:
                errors.append(f"Date error: {context} is earlier than {previous_context}")
            previous_date, previous_context = date, context

        return errors
