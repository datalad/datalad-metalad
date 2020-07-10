import yaml
import yaml.scanner


def categorize_word(token: str) -> str:
    parts = token.split("@")
    if len(parts) == 2 and len(parts[0]) > 0 and len(parts[1]) > 0:
        return f"<email>"
    return token


class YamlIndentationSanitizer(object):
    def __init__(self, document, known_keys=None):
        self.document = document
        self.known_keys = [] if known_keys is None else known_keys
        self.original_document = document
        self.corrections = set()
        self.over_indentation_fixed = False
        self.under_indentation_fixed = False
        self.missing_colons_fixed = True

    def sanitize(self) -> bool:
        fixed = False
        while not fixed:
            fixed = True
            fixed &= self._fix_missing_colons()
            fixed &= self._fix_over_indentation()
            fixed &= self._fix_under_indentation()
        return True

    def _possible_key(self, word: str) -> bool:
        if word.endswith(":"):
            word = word[:-1]
        return categorize_word(word) in self.known_keys

    def _fix_mapping(self, token: yaml.Token, previous_token: yaml.Token) -> bool:
        lines = self.document.splitlines()
        if isinstance(previous_token, yaml.ValueToken) and isinstance(token, yaml.ScalarToken):
            end_line = token.end_mark.line

            if token.start_mark.line != end_line:
                first_word = lines[end_line].split()[0]
                if self._possible_key(first_word):
                    self.corrections.add((end_line, f"assuming over-indented key: {first_word}"))
                    lines[end_line] = lines[end_line][1:]
                else:
                    start_line = token.start_mark.line
                    self.corrections.add(
                        (start_line,
                         f"assuming missing double-quotes in multiline value (lines {start_line}-{end_line}): "
                         f"{' '.join(map(str.strip, lines[start_line:end_line + 1]))}"))
                    first_word_index = len(lines[start_line]) - len(lines[start_line].lstrip())
                    lines[start_line] = lines[start_line][:first_word_index] + '"' + lines[start_line][first_word_index:]
                    lines[end_line] += '"'
            else:
                self.corrections.add((end_line, f"assuming missing double-quotes: {lines[end_line]}"))
                lines[end_line] = lines[end_line].replace(": ", ': "', 1)
                lines[end_line] += '"'

            self.document = "\n".join(lines)
            return True
        return False

    def _find_over_indentation(self):
        loader = yaml.SafeLoader(self.document)
        previous_token = None
        token = loader.get_token()
        while token is not None:
            try:
                new_token = loader.get_token()
            except yaml.YAMLError as yaml_error:
                if hasattr(yaml_error, "problem") \
                        and yaml_error.problem.startswith("mapping values are not allowed here"):
                    return token, previous_token
                raise
            previous_token = token
            token = new_token
        return None

    def _fix_over_indentation(self) -> bool:
        try:
            over_indented = self._find_over_indentation()
            while over_indented:
                if not self._fix_mapping(*over_indented):
                    self.over_indentation_fixed = False
                    return True
                over_indented = self._find_over_indentation()
            self.over_indentation_fixed = True
            return True
        except yaml.YAMLError:
            return False

    def _find_missing_colons(self):
        loader = yaml.BaseLoader(self.document)
        token = loader.get_token()
        while token is not None:
            try:
                new_token = loader.get_token()
            except yaml.YAMLError as yaml_error:
                if hasattr(yaml_error, "problem") \
                        and yaml_error.problem.startswith("could not find expected ':'"):
                    if hasattr(yaml_error, "context_mark"):
                        return yaml_error.context_mark.line
                    self.missing_colons_fixed = False
                    return None
                raise
            token = new_token
        return None

    def _fix_missing_colon_in_document(self, line):
        lines = self.document.splitlines()
        self.corrections.add((line, f"assuming missing '-' in list value: {lines[line].lstrip()}"))
        first_word_index = len(lines[line]) - len(lines[line].lstrip())
        lines[line] = lines[line][:first_word_index] + '- ' + lines[line][first_word_index:]
        self.document = "\n".join(lines)
        return True

    def _fix_missing_colons(self):
        try:
            missing_colon_line = self._find_missing_colons()
            while missing_colon_line:
                if not self._fix_missing_colon_in_document(missing_colon_line):
                    self.missing_colons_fixed = False
                    return True
                missing_colon_line = self._find_missing_colons()
            return True
        except yaml.YAMLError:
            return False

    def _find_under_indentation(self):
        loader = yaml.SafeLoader(self.document)
        current_level = []
        token = loader.get_token()
        while token is not None:
            if isinstance(token, yaml.BlockEndToken):
                next_token = loader.peek_token()
                if isinstance(next_token, (yaml.BlockMappingStartToken, yaml.BlockSequenceStartToken)):
                    return token.start_mark.line, current_level[-1] - token.start_mark.column
                else:
                    current_level.pop(-1)
            elif isinstance(token, (yaml.BlockMappingStartToken, yaml.BlockSequenceStartToken)):
                current_level.append(token.start_mark.column)
            token = loader.get_token()
        return None

    def _throwing_fix_under_indentation(self):
        under_indented = self._find_under_indentation()
        while under_indented:
            line, columns_to_add = under_indented
            self.corrections.add((line, f"assuming line is under-indented by {columns_to_add} columns."))
            lines = self.document.splitlines()
            lines[line] = " " * columns_to_add + lines[line]
            self.document = "\n".join(lines)
            under_indented = self._find_under_indentation()

    def _fix_under_indentation(self) -> bool:
        try:
            self._throwing_fix_under_indentation()
            self.under_indentation_fixed = True
            return True
        except yaml.scanner.ScannerError as error:
            return False


if __name__ == "__main__":
    test_doc = """

dataset:
  name: Rodent-Intelligence Brainscans
     location: juseless:/data/project/riskystudy
  author:
    - a@fz-juelich.de


study:
    name: sdfsdf: werwerw
 contributor:
           ssdsd
           sdsds
      a@b.c:
        given_name: 1
           last_name: 2

 contributor:
     werwer
     erwerw
   aasd: wew

keyword:
    - aaaa
  - bbbb
   - ccccc
    
publication:
   - title: a
     year: ya
     title: b
     year: yb
"""

    sanitizer = YamlIndentationSanitizer(test_doc, [
        "study",
        "name",
        "contributor",
        "location",
        "<email>",
        "given_name",
        "last_name",
        "keyword"])

    if not sanitizer.sanitize():
        print("Cant sanitize")

    for line, correction in sorted(list(sanitizer.corrections), key=lambda e: e[0]):
        print(f"line: {line + 1}: {correction}")

    if sanitizer.document != sanitizer.original_document:
        print(sanitizer.missing_colons_fixed, sanitizer.under_indentation_fixed, sanitizer.over_indentation_fixed)
        print("final document:")
        print(sanitizer.document)
