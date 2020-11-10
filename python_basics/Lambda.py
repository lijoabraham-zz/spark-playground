def sum(x,y):
    return x+y

print(sum(3,4))

res = lambda x,y :x+y
print(res(4,5))

n = [1,2,3]

sq = list(map(lambda x:x*2,n))
print(sq)

print(list(filter(lambda x : x > 2, n)))

