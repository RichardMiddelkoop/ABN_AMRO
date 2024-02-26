import tkinter
from tkinter import ttk, StringVar
from tkinter.filedialog import askopenfilename

class GUI:

    def __init__(self, window): 
        # 'StringVar()' is used to get the instance of input field
        self.input_text = StringVar()
        self.input_text1 = StringVar()
        self.input_countries = StringVar()
        self.database_path = ''
        self.database_path_2 = ''
        self.countries = ''

        window.title("Request Notifier")
        window.resizable(0, 0) # this prevents from resizing the window
        window.geometry("700x300")

        ttk.Button(window, text = "Database file 1", command = lambda: self.set_path_database_field()).grid(row = 0, ipadx=5, ipady=15) # this is placed in 0 0
        ttk.Entry(window, textvariable = self.input_text, width = 70).grid( row = 0, column = 1, ipadx=1, ipady=1) # this is placed in 0 1

        ttk.Button(window, text = "Database file 2", command = lambda: self.set_path_database_2_field()).grid(row = 1, ipadx=5, ipady=15) # this is placed in 0 0
        ttk.Entry(window, textvariable = self.input_text1, width = 70).grid( row = 1, column = 1, ipadx=1, ipady=1) # this is placed in 0 1

        ttk.Label(window, text = "Countrie(s) seperated by \";\"").grid(row = 2, ipadx=5, ipady=15) # this is placed in 0 0
        ttk.Entry(window, textvariable = self.input_countries, width = 70).grid( row = 2, column = 1, ipadx=1, ipady=1) # this is placed in 0 1
        
        ttk.Button(window, text = "Close", command=lambda: self.close_window(window)).grid(row = 3, ipadx=5, ipady=15) # this is placed in 0 0

    def set_path_database_field(self):
        self.database_path = askopenfilename() 
        self.input_text.set(self.database_path)

    def set_path_database_2_field(self):
        self.database_path_2 = askopenfilename()
        self.input_text1.set(self.database_path_2)

    def set_path_countries(self):
        self.countries = self.input_countries.get()
        self.input_countries.set(self.countries)
    
    def close_window(self, window):
        self.set_path_countries()
        window.quit()

    def get_database_path(self): 
        """ Function provides the databases full file path."""
        return self.database_path

    def get_database_path_2(self):
        """Function provides the second databases full file path."""
        return self.database_path_2

    def get_countries(self):
        """Function provides the countries to filter."""
        return str(self.countries)

def getFiles():
    """Helper function creates a popup to get user input for the path of the databases, and the countries to filter for"""
    window = tkinter.Tk()
    gui = GUI(window)
    window.mainloop()
    # Extracting the full file path for re-use. Two ways to accomplish this task is below. 
    return (str(gui.get_database_path()).strip(), str(gui.get_database_path_2()).strip(), str(gui.get_countries()).strip())