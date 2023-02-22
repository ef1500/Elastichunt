from dataclasses import asdict, is_dataclass

class PercentParser:
    """
    A class for parsing strings containing percent-encoded tokens.
    
    Percent-encoded tokens are of the form "%{key}", where "{key}" is a key
    that maps to a value in a lookup dictionary.
    """
    
    def __init__(self, fmt_token_repls, fmt_data):
        """
        Initializes a PercentParser object with the given format token replacements and format data.
        
        Args:
            fmt_token_repls: A dictionary containing the format token replacements.
                The keys are the tokens to replace, and the values are the corresponding keys
                in the format data dictionary.
            fmt_data: A dictionary or dataclass containing the format data.
                The keys are the values that the tokens will be replaced with,
                and the values are the corresponding values for those keys.
        """
        self.fmt_tokens = fmt_token_repls
        if is_dataclass(fmt_data):
            self.fmt_data = asdict(fmt_data)
        else:
            self.fmt_data = fmt_data

        self.repls = {key_a: fmt_data[value_a] for key_a, value_a in
                      fmt_token_repls.items() if value_a in fmt_data}

    def parse_string(self, input_string: str) -> str:
        """
        Parses the given input string, replacing any percent-encoded tokens
        with their corresponding values.

        Args:
            input_string: The input string to parse.

        Returns:
            The parsed string with any percent-encoded tokens 
            replaced by their corresponding values.
        """
        for key, value in self.repls.items():
            input_string = input_string.replace(f"%{key}", str(value))
        return input_string