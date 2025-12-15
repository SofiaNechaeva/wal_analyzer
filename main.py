from tkinter import Tk
from frontend import WalAnalyzerApp
from metabd import init_sqlite

def main():
    init_sqlite()
    root = Tk()
    app = WalAnalyzerApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()