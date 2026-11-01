with open('server.js', 'r') as f:
    content = f.read()

# Remove any 2024 clock checks - current year is 2026
content = content.replace("year !== 2024", "false")
content = content.replace("year !== 2024", "false")
content = content.replace("System clock is 2024", "System clock is 2026")
content = content.replace('"2024"', '"2026"')

with open('server.js', 'w') as f:
    f.write(content)

print('Fixed clock references to 2026')