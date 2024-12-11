import os
import pandas as pd

PATH = os.path.join(os.getcwd(), '..', '..', 'Domains', 'categories_all.csv')

def count_categories(data_frame):
    # Extrahiere die Kategorien aus der 'Category'-Spalte und teile sie auf
    categories = data_frame['category'].str.split('/')

    # Erstelle eine Liste aller Kategorien
    all_categories = [category.strip() for category_list in categories.dropna() for category in category_list]

    all_categories_c = [category.replace("&", "\&") for category in all_categories]

    # Zähle die Häufigkeit jeder Kategorie
    category_counts = pd.Series(all_categories_c).value_counts()

    # Gib die Ergebnisse zurück
    return category_counts

def count_top_categories(data_frame):
    """
    Counts only the top level categorie (e.g., News <- Business News
    """
    categories = data_frame['category'].str.split('/')

    all_categories = [categorie[1] for categorie in categories.dropna()]

    # Erstelle eine Liste aller Kategorien
    #all_categories = [category.strip() for category_list in categories.dropna() for category in category_list]

    print(len(all_categories))

    print(len(list(set(all_categories))))

    return categories

if __name__ == '__main__':
    df = pd.read_csv(PATH)
    #cc = count_categories(df)

    ctc = count_top_categories(df)
    #print(ctc)


    #print(cc.to_latex())

    #print(cc.head(10))
