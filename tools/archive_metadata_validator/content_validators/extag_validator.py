from typing import List

from messages import ValidatorMessage, WarningMessage, ObjectLocation
from .content_validator import ContentValidator


class ExTagValidator(ContentValidator):
    def _validate(self, spec: dict, prefix: str = "") -> List[ValidatorMessage]:
        result = []
        for key, value in spec.items():
            context = f"{prefix}.{key}" if prefix else f"{key}"
            if isinstance(value, dict):
                result += self._validate(value, context)
            else:
                if isinstance(value, str) and (value.startswith("<ex>") or value.endswith("</ex>")):
                    result += [
                        WarningMessage(
                            f"ex-tags around content ({value}), did you forget to alter the example content?",
                            ObjectLocation(self.file_name, context))]
        return result

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        return self._validate(spec)
