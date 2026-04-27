import re
import sys

try:
    with open("compile_errors.txt", "r", encoding="utf-16le", errors="ignore") as f:
        text = f.read()

    matches = set(re.findall(r"Cannot find name '([^']+)'", text))
    if not matches:
        print("No missing names found, or file format differs.")
    else:
        print("Missing variables to declare:", ", ".join(matches))
except Exception as e:
    print("Error:", e)
