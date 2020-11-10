class TestClass:
    def __init__(self, name, id):
        self.name = name
        self.id = id

details = TestClass('lijo',1)
print(details.id)
print(details.name)

help(TestClass)