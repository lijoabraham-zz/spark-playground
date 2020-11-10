import json

x = '{"name" : "lijo", "id" : 1}'

y = json.loads(x)

print(y)

z = json.dumps(y)

print(z)