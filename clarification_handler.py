class ClarificationHandler:
    """
    Handles scenarios where user queries are ambiguous.
    """
    def needs_clarification(self, response: str) -> bool:
        """
        Checks if the response indicates ambiguity.

        Args:
            response: The response string to inspect.
        Returns:
            True if clarification is needed, else False.
        """
        unclear_phrases = ["unsure", "cannot determine", "ambiguous"]
        return any(phrase in response.lower() for phrase in unclear_phrases)

    def request_clarification(self) -> str:
        """
        Prompts the user to provide additional details.
        """
        return input("Your request is ambiguous. Please provide more details: ")
