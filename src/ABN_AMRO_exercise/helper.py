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
        """
        Set the database path field.

        This method prompts the user to select a database file using a file dialog,
        sets the `database_path` attribute of the object to the selected file path,
        and updates the corresponding input text field with the selected path.

        :param self: The object instance.
        """
        self.database_path = askopenfilename() 
        self.input_text.set(self.database_path)

    def set_path_database_2_field(self):
        """
        Set the database path 2 field.

        This method prompts the user to select a database file using a file dialog,
        sets the `database_path_2` attribute of the object to the selected file path,
        and updates the corresponding `input text1` field with the selected path.

        :param self: The object instance.
        """
        self.database_path_2 = askopenfilename()
        self.input_text1.set(self.database_path_2)

    def set_path_countries(self):
        """
        Set the database path field.

        This method prompts the user to select a database file using a file dialog,
        sets the `countries` attribute of the object to the selected file path,
        and updates the corresponding `input_countries` field with the selected path.

        :param self: The object instance.
        """
        self.countries = self.input_countries.get()
        self.input_countries.set(self.countries)
    
    def close_window(self, window):
        """
        Close the window and set path countries.

        This method sets the path countries and then quits the window.

        :param self: The object instance.
        :param window: The window to close.
        """
        self.set_path_countries()
        window.quit()

    def get_database_path(self):
        """
        Get the database path.

        :param self: The object instance.
        :return: The database path.
        :rtype: str
        """
        return self.database_path

    def get_database_path_2(self):
        """
        Get the second database path.

        :param self: The object instance.
        :return: The second database path.
        :rtype: str
        """
        return self.database_path_2

    def get_countries(self):
        """"
        Get the user input of countries.

        :param self: The object instance.
        :return: The user input of countries.
        :rtype: str
        """
        return str(self.countries)

def getFiles():
    """
    Get files and countries through a graphical user interface.

    This function creates a Tkinter window, initializes a GUI object, and starts the main event loop.
    It then extracts the full file paths and countries selected by the user from the GUI object.

    :return: A tuple containing the full file paths and countries selected by the user.
    :rtype: tuple
    """
    window = tkinter.Tk()
    gui = GUI(window)
    window.mainloop()
    # Extracting the full file path for re-use. Two ways to accomplish this task is below. 
    return (str(gui.get_database_path()).strip(), str(gui.get_database_path_2()).strip(), str(gui.get_countries()).strip())