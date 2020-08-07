class bitarray:

    def __init__(self, size):
        self.arr = [0] * size
        

    def setall(self, bit):

        if bit == 0 or bit == 1:
            for i in range(len(self.arr)):
                self.arr[i] = bit
        else:
            raise Exception("Invalid bit entered, only 1 or 0 are valid inputs.")


    def set(self, n, bit):

        if bit == 0 or bit == 1:

            try:
                self.arr[n] = bit
            except IndexError:
                raise Exception("The value entered is out of range of the array.")
            except TypeError:
                raise Exception("Please enter a value that is an integer")

        else:
            raise Exception("Invalid bit entered, only 1 or 0 are valid inputs.")



    def get(self, n):

        try:
            return self.arr[n]
        except IndexError:
            raise Exception("The value entered is out of range of the array.")
        except TypeError:
            raise Exception("The value entered is not an integer, Please enter a value that is an integer")