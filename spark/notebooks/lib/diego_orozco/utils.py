import math

class BankAccount:
  def __init__(self, amount):
      self.amount = amount

  def __str__(self):
      return f"La cuenta tiene {self.amount}"

  def __repr__(self):
      return f"BankAccount('{self.amount}')"
  
  def __deposit(self, amount):
    if (amount>0):
        self.amount += amount
        print("cantidad actualizada")
        print(f"BankAccount amount='{self.amount}'")
    else:
        print("cantidad debe ser mayor a 0")
    return self.amount
    
  def __withdraw(self, amount):
    if ((-amount)<0):
        if (amount<self.amount):
            self.amount -= amount
            print("cantidad actualizada")
            print(f"BankAccount amount='{self.amount}'")
        else:
            print("Fondos insuficientes")
    else:
        print("cantidad debe ser mayor a 0")
    return self.amount
      

  def process(self, acction, amount):
      get_process= {
          "deposit": self.__deposit,
          "withdraw": self.__withdraw,
      }
      return get_process[acction](amount)
  
