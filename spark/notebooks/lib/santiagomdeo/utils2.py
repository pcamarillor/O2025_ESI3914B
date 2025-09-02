class BankAccount:
    def __init__(self, initial_money):
        self.money = initial_money

    @staticmethod
    def deposit(money, quantity_added):
        return money + quantity_added
    @staticmethod
    def withdraw(money, quantity_to_withdraw):
        if money > quantity_to_withdraw:
            return money - quantity_to_withdraw
        else:
            raise ValueError("you are kinda broke (not enough funds)")
        
    operations = {
        'deposit': deposit,
        'withdraw': withdraw
    }

    def process(self, func, quantity):
        self.money = operations[func]