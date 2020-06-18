from typing import Dict, List, Union

from messages import ValidatorMessage, WarningMessage, ObjectLocation
from .content_validator import ContentValidator


class ExTagValidator(ContentValidator):
    def _validate(self, spec: Union[Dict, List], prefix: str = "") -> List[ValidatorMessage]:
        result = []
        if isinstance(spec, dict):
            for key, value in spec.items():
                result += self._validate(value, f"{prefix}.{key}" if prefix else f"{key}")
        elif isinstance(spec, list):
            for index, item in enumerate(spec):
                result += self._validate(item, f"{prefix}[{index}]" if prefix else f"[{index}]")
        elif isinstance(spec, str):
            if spec.startswith("<ex>") or spec.endswith("</ex>"):
                result += [
                    WarningMessage(
                        f"ex-tags around content ({spec}), did you forget to alter the example content?",
                        ObjectLocation(self.file_name, prefix))]
        return result

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        return self._validate(spec)
