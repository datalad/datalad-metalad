from typing import Dict, List, Tuple, Union

from messages import ValidatorMessage, WarningMessage, ObjectLocation, StringLocation
from .content_validator import ContentValidator


MAX_EXTAG_WARNINGS = 5


class ExTagValidator(ContentValidator):
    def _validate(self, spec: Union[Dict, List], prefix: str = "") -> List[Tuple[str, str]]:
        result = []
        if isinstance(spec, dict):
            for key, value in spec.items():
                result += self._validate(value, f"{prefix}.{key}" if prefix else f"{key}")
        elif isinstance(spec, list):
            for index, item in enumerate(spec):
                result += self._validate(item, f"{prefix}[{index}]" if prefix else f"[{index}]")
        elif isinstance(spec, str):
            if spec.startswith("<ex>") or spec.endswith("</ex>"):
                result += [(spec, prefix)]
        return result

    def perform_validation(self, spec: dict) -> List[ValidatorMessage]:
        extag_locations = self._validate(spec)
        if len(extag_locations) <= MAX_EXTAG_WARNINGS:
            return list([
                WarningMessage(
                    f"ex-tags in ({content}), did you forget to remove them?",
                    ObjectLocation(self.file_name, dotted_name))
                for content, dotted_name in extag_locations])
        else:
            example_content, example_dotted_name = extag_locations[0]
            return [
                WarningMessage(
                    f"ex-tags found in {len(extag_locations)} text strings, "
                    f"for example ``{example_dotted_name}: {example_content}´´, "
                    f"did you forget to remove them?",
                    StringLocation(f"{self.file_name}:<multiple places>"))]
