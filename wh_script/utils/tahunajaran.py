class TahunAjaran:
    def __init__(self, ta):
        self.ta = ta
    
    def get_string(self):
        return str(self.ta)
    
    @staticmethod
    def get_periode_sebelum(ta):
        tahun = int(ta[0:4])
        periode = int(ta[4:5])
        if periode == 1:
            return str(tahun-1) + "2"
        elif periode == 2:
            return str(tahun) + "1"
    
    @staticmethod
    def is_valid_ta_format(ta):
        valid_suffix = [1,2]
        ta = str(ta)
        try:
            if len(ta) == 5:
                if int(ta[0:4]) > 1000:
                    if int(ta[4:]) in valid_suffix:
                        return True
            return False
        except TypeError:
            return False
        except ValueError:
            return False