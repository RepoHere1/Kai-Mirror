with open('server.js', 'r') as f:
    content = f.read()

idx = content.find('p === "/api/strategy/')
if idx >= 0:
    print(content[idx:idx+500])