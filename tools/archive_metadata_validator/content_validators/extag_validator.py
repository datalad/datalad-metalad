from typing import List, Tuple, Union

from messages import ValidatorMessage, WarningMessage, StringLocation
from .content_validator import ContentValidator


MAX_EXTAG_REPORTS = 5


class ExTagValidator(ContentValidator):
    @staticmethod
    def _validate(spec: Union[dict, list], prefix: str = "") -> List[Tuple[str, str]]:
        result = []
        if isinstance(spec, dict):
            for key, value in spec.items():
                result += ExTagValidator._validate(value, f"{prefix}.{key}" if prefix else f"{key}")
        elif isinstance(spec, list):
            for index, item in enumerate(spec):
                result += ExTagValidator._validate(item, f"{prefix}[{index}]" if prefix else f"[{index}]")
        elif isinstance(spec, str):
            if spec.startswith("<ex>") or spec.endswith("</ex>"):
                result += [(spec, prefix)]
        return result

    def perform_validation(self) -> List[ValidatorMessage]:
        ex_tag_locations = self._validate(self.spec)
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
                StringLocation(f"{self.file_name}"))]
        return []
