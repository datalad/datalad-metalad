from typing import cast

from .documentedinterface import ParameterEntry


class PipelineElement:

    @classmethod
    def check_keyword_args(cls, keyword_args):
        for key, value in keyword_args.items():
            parameter_entry = cast(
                ParameterEntry,
                cls.interface_documentation.parameter_entry_by_name[key])

            if parameter_entry.constraints:
                parameter_entry.constraints(value)
