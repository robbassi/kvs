# make an array

class bitarray:
    def __init__(self, size):
        self.arr = [0] * size
        

    def setall(self, bit):
        if bit == 0 or bit == 1:
            for i in range(len(self.arr)):
                self.arr[i] = bit
        else:
            raise Exception("Invalid bit entered, only 1 or 0 are valid inputs.")

    # def set(self, n, bit)

    # def get(self, n)