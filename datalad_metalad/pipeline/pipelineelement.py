from typing import (
    Any,
    Optional,
    cast,
)

from .documentedinterface import ParameterEntry


class PipelineElement:

    interface_documentation = None

    @classmethod
    def check_keyword_args(cls, keyword_args) -> Optional[str]:
        if not cls.interface_documentation:
            return

        messages = []
        for key, value in keyword_args.items():
            parameter_entry = cast(
                ParameterEntry,
                cls.interface_documentation.parameter_entry_by_name[key])

            if parameter_entry.constraints:
                try:
                    parameter_entry.constraints(value)
                except ValueError as ve:
                    messages.append(f"{cls.__name__}.{key}: {ve})")

        if messages:
            return "\n".join(messages) + "\n"
        return None

    @classmethod
    def get_keyword_arg_value(cls, keyword_arg, value) -> Any:
        if not cls.interface_documentation:
            return value

        parameter_entry = cast(
            ParameterEntry,
            cls.interface_documentation.parameter_entry_by_name[keyword_arg])

        if parameter_entry.constraints:
            return parameter_entry.constraints(value)
        return value
