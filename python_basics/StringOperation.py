x = "This is string testing"

print(x[0:5])

print(x[:-1])

print(x[1:])
print(x.upper())
print(x.lower())
print(x.startswith("hello "))
print(x.endswith("testing"))
print(x.replace("testing", "learning"))
print(x.replace("testing", "learning1", 1))
print(x.lstrip())
print(x.rstrip())
print(x.strip())
print(x.ljust(30, "-"))
print(x.rjust(30, "-"))
print(x.center(30, "-"))
print(x.count('e'))
print(x.index('e'))
print(x.upper()[3:].startswith("S"))



