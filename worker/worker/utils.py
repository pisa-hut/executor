import os


def resolve_host_path(host_path: str) -> str:
    """
    Resolve a host path, expanding user and environment variables, and converting to an absolute path.

    Args:
        host_path (str): The input host path to resolve.
    Returns:
        str: The resolved absolute host path.
    """

    SBSVF_DIR = os.getenv("SBSVF_DIR", "/opt/sbsvf")

    if not os.path.isabs(host_path):
        host_path = os.path.join(SBSVF_DIR, host_path)
    expanded_path = os.path.expandvars(os.path.expanduser(host_path))
    absolute_path = os.path.abspath(expanded_path)
    return absolute_path
