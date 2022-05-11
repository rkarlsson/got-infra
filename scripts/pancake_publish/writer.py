import traceback
import ctypes


class t_tob_state(ctypes.Structure):
    _fields_ = [
       ("id",    ctypes.c_int),
       ("price", ctypes.c_double)
    ]
    @staticmethod
    def alloc(sym_id:int, price:float):
        return t_tob_state(sym_id, price)

class Writer(object):

    def __init__(self):
        try:
            self.library = ctypes.cdll.LoadLibrary("./libaeron_writer.so")
         
            """ alloc/dealloc of episode instance pointers: """
            self.library.new_instance.restype = ctypes.c_void_p
            # self.library.del_instance.restype = ctypes.c_int32

            self.library.send_tob.restype  = ctypes.c_int32
            self.library.send_tob.argtypes = [ctypes.c_void_p, ctypes.c_void_p] 

            self._instance = self.library.new_instance()

        except:
            traceback.print_exc()

    def send_tob(self, state:t_tob_state):
        self.library.send_tob(self._instance, ctypes.byref(state))

if __name__ == "__main__":
  print("hello python")
  w = Writer()
  tob = t_tob_state.alloc(0)
  w.send_tob(tob)

