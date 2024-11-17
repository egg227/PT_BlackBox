import sqlite3
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import List, Set
from urllib.parse import urlparse, urljoin
import argparse

def setup_database(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS links (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT NOT NULL)''')
    conn.commit()

def download_page(url: str) -> str:
    try:
        with urllib.request.urlopen(url) as response:
            return response.read().decode('utf-8')
    except urllib.error.URLError as e:
        print(f"Ошибка скачивания страницы {url}: {e}")
        return ""


class WikiLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: Set[str] = set()

    def handle_starttag(self, tag: str, attrs: List[tuple]):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href' and value.startswith('/wiki/'):
                    full_url = urljoin("https://ru.wikipedia.org", value)
                    self.links.add(full_url)

    def feed_page(self, page_content: str):
        self.feed(page_content)


def extract_links(url: str) -> Set[str]:
    page_content = download_page(url)
    if page_content:
        parser = WikiLinkParser()
        parser.feed_page(page_content)
        return parser.links
    return set()


def save_links_to_db(conn: sqlite3.Connection, links: Set[str]) -> int:
    cursor = conn.cursor()
    saved_count = 0
    try:
        cursor.execute('BEGIN TRANSACTION')
        for link in links:
            cursor.execute('INSERT OR IGNORE INTO links (url) VALUES (?)', (link,))
            saved_count += 1
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при добавлении ссылок в базу: {e}")
        conn.rollback()
    return saved_count


def crawl_links(conn: sqlite3.Connection, start_url: str, depth: int, visited: Set[str], all_links: Set[str], max_depth: int = 6) -> None:
    if depth > max_depth:
        return

    if start_url in visited:
        return

    visited.add(start_url)
    all_links.add(start_url)

    print(f"Глубина {depth}, обрабатываем: {start_url}")
    
    links = extract_links(start_url)

    new_links = links - all_links
    saved_count = save_links_to_db(conn, new_links)
    print(f"Сохранено {saved_count} новых ссылок.")

    for link in links:
        crawl_links(conn, link, depth + 1, visited, all_links, max_depth)

def main(url: str, db_name: str = "wiki_links.db"):
    print(f"Начинаем сбор ссылок с {url}")
    
    conn = sqlite3.connect(db_name)
    setup_database(conn) 
    
    visited = set()
    all_links = set()
    
    crawl_links(conn, url, depth=1, visited=visited, all_links=all_links)
    
    conn.close()

def parse_args():
    parser = argparse.ArgumentParser(description="Скрипт для сбора и сохранения ссылок с страницы Википедии.")
    parser.add_argument("url", help="URL начальной страницы для сбора ссылок.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    start_url = args.url  
    main(start_url)
