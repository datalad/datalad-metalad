from typing import Dict, List, Tuple, Union

from messages import ValidatorMessage, WarningMessage, ObjectLocation, StringLocation
from .content_validator import ContentValidator


MAX_EXTAG_REPORTS = 5


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
        ex_tag_locations = self._validate(spec)
        if ex_tag_locations:
            return [WarningMessage((
                f"ex-tags found in {len(ex_tag_locations)} text strings"
                + (":\n"
                   if len(ex_tag_locations) <= MAX_EXTAG_REPORTS
                   else f" (showing the first {MAX_EXTAG_REPORTS}):\n")
                + ("\n".join([
                    f"{example_dotted_name}: {example_content}"
                    for example_content, example_dotted_name in ex_tag_locations[:MAX_EXTAG_REPORTS]]))
                + ("\n  ..." if len(ex_tag_locations) > MAX_EXTAG_REPORTS else "")),
                StringLocation(f"{self.file_name}:<multiple places>"))]
        return []
