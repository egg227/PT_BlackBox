import pytest
import sqlite3
from crawler import save_links_to_db, setup_database, WikiLinkParser

@pytest.fixture
def db_connection():
    """Фикстура для временной базы данных"""
    conn = sqlite3.connect(':memory:')  
    setup_database(conn) 
    yield conn  
    conn.close()  

def test_save_links_to_db_single_insert(db_connection):
    links = {"https://ru.wikipedia.org/wiki"}
    saved_count = save_links_to_db(db_connection, links)
    assert saved_count == 1  

    cursor = db_connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM links WHERE url = "https://ru.wikipedia.org/wiki"')
    result = cursor.fetchone()
    assert result[0] == 1  

def test_save_links_to_db_duplicate_insert(db_connection):
    links = {"https://ru.wikipedia.org/wiki", "https://ru.wikipedia.org/wiki"}
    saved_count = save_links_to_db(db_connection, links)
    assert saved_count == 1  

    cursor = db_connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM links WHERE url = "https://ru.wikipedia.org/wiki"')
    result = cursor.fetchone()
    assert result[0] == 1  

def test_save_multiple_links(db_connection):
    links = {"https://ru.wikipedia.org/wiki", "https://en.wikipedia.org/wiki/Michael_Jordan", "https://en.wikipedia.org/wiki/Lexus"}
    saved_count = save_links_to_db(db_connection, links)
    assert saved_count == 3  

    cursor = db_connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM links WHERE url IN (?, ?, ?)', 
                   ("https://ru.wikipedia.org/wiki", "https://en.wikipedia.org/wiki/Michael_Jordan", "https://en.wikipedia.org/wiki/Lexus"))
    result = cursor.fetchone()
    assert result[0] == 3  

def test_save_links_ignore_duplicates(db_connection):
    links = {"https://ru.wikipedia.org/wiki", "https://en.wikipedia.org/wiki/Michael_Jordan", "https://ru.wikipedia.org/wiki"}
    saved_count = save_links_to_db(db_connection, links)
    assert saved_count == 2  

    cursor = db_connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM links WHERE url IN (?, ?)', 
                   ("https://ru.wikipedia.org/wiki", "https://en.wikipedia.org/wiki/Michael_Jordan"))
    result = cursor.fetchone()
    assert result[0] == 2  
