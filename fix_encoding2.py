import re

# Fix encoding issues in HTML files with BOM handling
replacements = {
    'EUR"': '-',      # em dash
    '...': '...',      # ellipsis
    '-': '-',       # middle dot
    ''': "'",       # apostrophe
    '"': '"',       # opening quote
    'EUR': '"',        # closing quote
    'EUR': '"',        # closing quote (alt)
    '': '',          # stray
    'EUR"': '-',      # em dash (alt)
    'EUR ': '- ',     # em dash with space
    '"': '"',       # opening quote
    '': "'",       # single quote
    ''': "'",       # single quote
}

files = [
    'dry_home.html',
    'live_home.html',
]

for fname in files:
    path = f'C:\\Users\\darri\\Kai-Mirror\\{fname}'
    try:
        # Try UTF-16 first (BOM files)
        with open(path, 'r', encoding='utf-16') as f:
            content = f.read()

        original = content
        for bad, good in replacements.items():
            content = content.replace(bad, good)

        if content != original:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed (UTF-16): {fname}")
        else:
            print(f"No changes: {fname}")
    except Exception as e:
        print(f"Error {fname}: {e}")

print("Done!")