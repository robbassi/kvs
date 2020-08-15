import array
import math

class bitarray:
    def __init__(self, size):
        self.arr = array.array('B', [0] * math.ceil(size/8))
        
    def setall(self, bit):
        if bit == 0 or bit == 1:
            self.arr = [bit] * len(self.arr)
        else:
            raise ValueError("SetAll: Invalid bit entered, only 1 or 0 are valid inputs.")

    def set(self, n, bit):
        if bit == 0 or bit == 1:
            try:
                self.arr[n] = bit
            except IndexError:
                raise IndexError("Set: Value entered is out of range of the array.")
            except TypeError:
                raise TypeError("Set: Enter a value that is an integer")
        else:
            raise Exception("Set: Invalid bit entered, only 1 or 0 are valid inputs.")

    def get(self, n):
        try:
            return self.arr[n]
        except IndexError:
            raise IndexError("Get: Value entered is out of range of the array.")
        except TypeError:
            raise TypeError("Get: Value entered is not an integer.")
