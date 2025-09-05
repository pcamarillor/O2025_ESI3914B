


class BankAccount:
    def __init__(self,money):
        self.money = money
        self.operations = {
        'deposit':self.deposit,
        'withdraw':self.withdraw,
        'get_balance':self.get_balance
        }

    def deposit(self,deposito):
        self.money += deposito
        print(f"Nuevo saldo: {self.money}")
    
    def withdraw(self,retiro):
        if(retiro<0):
            print(f"No puedes retirar negativo o 0")
        elif (retiro<self.money):
            self.money -= retiro
            print(f"Dinero Retirado: {retiro}")
        else:
            print("No cuenta con fondos suficientes")

    def get_balance(self):
        print(f"Saldo: {self.money}")

    def process(self,operation,*args): 
        operation = self.operations.get(operation)
        operation(*args)
    
