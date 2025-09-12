class BankAccount:
    def __init__(self, balance):
        self.update_balance = balance
        self.dict_operations = {
            'deposit': self.deposit,
            'withdraw' : self.withdraw,
            'get_balance' : self.get_balance}
    
    
    def get_balance(self):
        return f"* Su balance es: $ {self.update_balance}"
    
    
    def deposit(self,value):
        self.update_balance += value
        return f"* Su depósito de $ {value} ha sido realizado con éxito. \n Su balance es: $ {self.update_balance}"
    
    
    def withdraw(self,value):
        if self.update_balance < value:
            return f"* No hay fondos suficientes."
        else:
            self.update_balance -= value
            return f"* Su retiro de $ {value} ha sido realizado con éxito. \n Su balance es: $ {self.update_balance}"
    
    def process(self,operations,*args):
        return self.dict_operations[operations](*args)
    
    