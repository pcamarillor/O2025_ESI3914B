class BankAccount:
    def __init__(self, balance):
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount

    def withdraw(self, amount):
        self.balance -= amount

    def process(self, action, amount):
        actions = {
        'deposit': self.deposit,
        'withdraw': self.withdraw
    }
        actions[action](amount)
        print(self.balance)


    