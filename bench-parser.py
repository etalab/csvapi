import warnings
from pathlib import Path

from csvapi.parser import detect_encoding
from csvapi.parser import from_csv
from csvapi.parser import detect_type, CSV_FILETYPES

SNIFF_LIMIT = 4096
FILES_DIR = "/Users/alexandre/Developer/Etalab/decapode/data/downloaded"

parsed = 0
errors = 0
warning = 0
not_csv = 0


for filepath in Path(FILES_DIR).glob("*.csv"):
    print('-----', filepath)
    parsed += 1
    file_type = detect_type(filepath)
    if not any([supported in file_type for supported in CSV_FILETYPES]):
        print(f"Not a CSV through magic number", file_type.strip())
        not_csv += 1
        continue
    encoding = detect_encoding(filepath)
    try:
        with warnings.catch_warnings(record=True) as w:
            table = from_csv(filepath, encoding=encoding, sniff_limit=SNIFF_LIMIT)
            if any(["Column" in _w.message.__str__() for _w in w]):
                warning += 1
    except Exception as e:
        print("ERROR", e)
        errors += 1

print(f"Errors: {errors}/{parsed} ({round(errors / parsed * 100, 2)}%)")
print(f"Column warnings: {warning}/{parsed} ({round(warning / parsed * 100, 2)}%)")
print(f"Not CSV (magic): {not_csv}/{parsed} ({round(not_csv / parsed * 100, 2)}%)")
