class bitarray:

    def __init__(self, size):
        self.arr = [0] * size
        

    def setall(self, bit):

        if bit == 0 or bit == 1:
            for i in range(len(self.arr)):
                self.arr[i] = bit
        else:
            raise Exception("SetAll: Invalid bit entered, only 1 or 0 are valid inputs.")


    def set(self, n, bit):

        if bit == 0 or bit == 1:

            try:
                self.arr[n] = bit
            except IndexError:
                raise Exception("Set: Value entered is out of range of the array.")
            except TypeError:
                raise Exception("Set: Enter a value that is an integer")

        else:
            raise Exception("Set: Invalid bit entered, only 1 or 0 are valid inputs.")



    def get(self, n):

        try:
            return self.arr[n]
        except IndexError:
            raise Exception("Get: Value entered is out of range of the array.")
        except TypeError:
            raise Exception("Get: Value entered is not an integer.")
