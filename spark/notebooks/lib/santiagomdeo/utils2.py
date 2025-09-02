

def deposit(money, quantity_added):
    return money + quantity_added

def withdraw(money, quantity_to_withdraw):
    if money > quantity_to_withdraw:
        return money - quantity_to_withdraw
    else:
        raise ValueError("you are kinda broke (not enough funds)")



class BankAccount:
    def __init__(self, initial_money):
        self.__money = initial_money
        self.operations = {
            'deposit': deposit,
            'withdraw': withdraw
        }
    
    @property
    def money(self):
        return self.__money
    

    def process(self, func, quantity):
        self.__money = self.operations[func](self.__money, quantity)
        return self.__money 