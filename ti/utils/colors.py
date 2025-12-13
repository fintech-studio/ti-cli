from enum import StrEnum

# ANSI 顏色碼
class Colors(StrEnum):
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'

def colorize(text:str, color:str) -> str:
    return f"{color}{text}{Colors.RESET}"