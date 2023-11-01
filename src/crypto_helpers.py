def convert_numeric_to_ethereum_hash(numeric_hash):
    """
    Ethereum transaction hashes are loaded into polars as numeric string types. This function converts the numeric string to a hex string and then prepends '0x'
    to the string to restore the transaction hash. If the conversion fails due to a large number, it returns '0x0' as a fallback.
    """
    try:
        hex_string = hex(int(numeric_hash))
        ethereum_hash = "0x" + hex_string[2:]
        return ethereum_hash
    except (ValueError, OverflowError):
        # Handle the case where the number is too big
        return "0x0"
