import uuid
import tempfile
import os

class FileTemporaryHandler:
    def __init__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.history = {}
    
    def generate_temporary_file(self, tag, extension):
        filename = str(uuid.uuid1()) + f".{extension}"
        full_path = os.path.join(self.temp_dir.name, filename)
        self.history[tag] = full_path
        return full_path
    
    def get_history_file(self):
        return self.history
    
    def get_temporary_file(self, tag):
        return self.history.get(tag)

    def clear_temp_folder(self):
        self.temp_dir.cleanup()

# a = FileTemporaryHandler()
# a.generate_temporary_file("abc","csv")
# a.generate_temporary_file("def","csv")
# print(a.get_temporary_file("abc"))
# print(a.get_history_file())
# a.clear_temp_folder()