import sys

class StdOutToFile:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.previous_stdout = sys.stdout
        file = open(self.filename, 'w')
        sys.stdout = file
        self.current_stdout = file
        
    def __exit__(self, excep_val, excep_type, excep_traceb):
        sys.stdout = self.previous_stdout
        self.current_stdout.close()
        return False

    
