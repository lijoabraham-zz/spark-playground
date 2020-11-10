### Lists
my_list = ['this','is','new', 'programming', 'language']
print(len(my_list))

print('python' in my_list)
my_list.append('python')
print(my_list)

my_list.extend(['java','php'])
print(my_list)

my_list.remove('php')

print(my_list)

copy_list = my_list
copy_list.remove('java')
print(copy_list == my_list)

c_list = my_list.copy()
c_list.remove('new')
print(c_list == my_list)

##Tuples

my_tuple = ('this','is','new', 'programming', 'language')
# my_tuple.append('java')
print(my_tuple)

items = [('a', 1), ('b', 2)]
for (a,b) in items:
    print(a,b)

uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
lowercase = 'abcdefghijklmnopqrstuvwxyz'

for ind in range(len(uppercase)):
    print(ind,uppercase[ind],lowercase[ind])