import re

class Utils:
    @staticmethod
    def sanitize_input(user_input: str) -> str:
        """
        Sanitizes user input to remove potentially harmful characters.

        Args:
            user_input: The raw input string.
        Returns:
            A cleaned-up string.
        """
        sanitized = user_input.strip()
        # Remove non-word characters except basic punctuation
        sanitized = re.sub(r"[^\w\s\.,;:'\"()\-]", "", sanitized)
        return sanitized
