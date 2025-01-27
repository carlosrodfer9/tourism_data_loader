import pandas as pd


def load_airport_data(path: str):
    data = pd.read_csv(path, sep=";")
    
