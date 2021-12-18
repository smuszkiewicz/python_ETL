from argparse import ArgumentParser
from sqlite3 import connect
import time

tracks_tablse_stmt = """
    CREATE TABLE IF NOT EXISTS tracks(
        id_wykonania VARCHAR(20) PRIMARY KEY,
        id_utworu VARCHAR(20),
        nazwa_artysty VARCHAR(20),
        tytul VARCHAR(20)
    )"""

sample_tablse_stmt = """
    CREATE TABLE IF NOT EXISTS sample(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_uzytkownika VARCHAR(20),
        id_utworu VARCHAR(20),
        data VARCHAR(20)
    )"""

t_insert_stmt = 'INSERT INTO tracks(id_wykonania, id_utworu, nazwa_artysty, tytul) VALUES(?,?,?,?)'

s_insert_stmt = 'INSERT INTO sample(id_uzytkownika, id_utworu, data) VALUES(?,?,?)'

def argument_parsing():
    parser = ArgumentParser(description='Podaj ścieżki plików')
    parser.add_argument(
        '--path_tracks', '-pt', type=str, required=True,
        help='Ścieżka pliku "unique_tracks'
    )
    parser.add_argument(
        '--path_sample', '-ps', type=str, required=True,
        help='Ścieżka pliku triplets_sample'
    )
    parser.add_argument(
        '--path_db', '-pdb', type=str, required=True,
        help='Ścieżka bazy danych'
    )
    paths = parser.parse_args()
    return paths

def extract(path):
    array = []
    with open(path, 'r', encoding='ISO-8859-1') as fd:
        for el in fd:
            array.append(el)
    return array

def transform(array):
    array2 = []
    for el in array:
        array2.append(el.strip('/n').split('<SEP>'))
    return array2

def load(path, tablse1, tablse2, insert1, insert2, data1, data2):
    with connect(path) as db_connector:
        db_connector.execute(tablse1)
        db_connector.execute(tablse2)

        db_cursor = db_connector.cursor()
        db_cursor.executemany(insert1, data1)
        db_cursor.executemany(insert2, data2)

def info(path):
    with connect(path) as db_connector:
        db_cursor = db_connector.cursor()
        print('Artysta z największą liczbą odsłuchań:')
        start = time.time()
        for entry in db_cursor.execute('SELECT nazwa_artysty, COUNT(sample.id_utworu) FROM tracks NATURAL JOIN sample GROUP BY nazwa_artysty ORDER BY COUNT(sample.id_utworu) DESC LIMIT 1'):
            print(entry)
        end = time.time()
        print('Czas przetwarzania danych:')
        print(end - start + ' s')
        print('5 najpopularniejszych utworów:')
        start = time.time()
        for entry in db_cursor.execute('SELECT tytul, COUNT(sample.id_utworu) FROM tracks NATURAL JOIN sample GROUP BY tytul ORDER BY COUNT(sample.id_utworu) DESC LIMIT 5'):
            print(entry)
        end = time.time()
        print('Czas przetwarzania danych:')
        print(end - start + ' s')


def main():
    paths = argument_parsing()
    print('Co chcesz zrobić?\n1 - Załadować dane do bazy danych\n2 - Wypisać informacje na wyjście')
    choice = input()
    if choice == '1':
        array_t = extract(paths.path_tracks)
        array_s = extract(paths.path_sample)
        array_t = transform(array_t)
        array_s = transform(array_s)

        load(
            paths.path_db, tracks_tablse_stmt,
            sample_tablse_stmt, t_insert_stmt,
            s_insert_stmt, array_t, array_s
        )
    elif choice == '2':
        info(paths.path_db)


if __name__== '__main__':
    main()