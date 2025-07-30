# tools/flagger.py

class FlaggedError(Exception):
    def __init__(self, code: str, context: dict):
        self.code = code
        self.context = context
        msg = f"[{code}] {context}"
        super().__init__(msg)


class Flagger:
    def __init__(self):
        self.errors: list[tuple[str, dict]] = []
        self.warnings: list[tuple[str, dict]] = []

    def error(self, code: str, context: dict) -> None:
        self.errors.append((code, context))
        raise FlaggedError(code, context)

    def warning(self, code: str, context: dict) -> None:
        self.warnings.append((code, context))

    def get_errors(self) -> list[tuple[str, dict]]:
        return self.errors

    def get_warnings(self) -> list[tuple[str, dict]]:
        return self.warnings

    def clear(self) -> None:
        self.errors.clear()
        self.warnings.clear()
