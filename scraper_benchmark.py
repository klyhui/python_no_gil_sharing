import aiohttp
import asyncio
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from argparse import ArgumentParser
from urllib.parse import urljoin

# Use books.toscrape.com for a stable benchmark
# It has 50 pages, 20 books per page.
BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"
# Base for resolving relative links from the catalogue
BASE_URL_FOR_JOINING = "https://books.toscrape.com/catalogue/"


async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, timeout=100) as response:
        return await response.text()


def parse_books_list(html: str) -> list[dict]:
    """Parses the list of books from a catalogue page."""
    soup = BeautifulSoup(html, "html.parser")
    books = []
    for item in soup.select("article.product_pod"):
        title_tag = item.select_one("h3 > a")
        if title_tag:
            title = title_tag['title'].strip()
            # Links are relative, so we must join them with the base
            link = urljoin(BASE_URL_FOR_JOINING, title_tag['href'])
            books.append({"title": title, "link": link})
    return books


def parse_book_details(html: str) -> dict:
    """Parses the details from a single book's page."""
    soup = BeautifulSoup(html, "html.parser")
    details = {}

    price_tag = soup.select_one(".product_main .price_color")
    stock_tag = soup.select_one(".product_main .availability")
    desc_tag = soup.select_one("#product_description + p")

    if price_tag:
        details["price"] = price_tag.text.strip()
    if stock_tag:
        details["stock"] = stock_tag.text.strip()
    if desc_tag:
        # Just get length to simulate a CPU-bound parsing task
        details["description_length"] = len(desc_tag.text.strip())

    return details


async def fetch_book_with_details(
    session: aiohttp.ClientSession, book: dict
) -> dict:
    """Fetches a book's detail page and parses it."""
    detail_html = await fetch(session, book["link"])
    # Run CPU-bound parsing in the same thread (blocking)
    book["details"] = parse_book_details(detail_html)
    return book


async def worker(queue: Queue, all_books: list) -> None:
    async with aiohttp.ClientSession(trust_env=True) as session:
        while True:
            async with asyncio.TaskGroup() as tg:
                try:
                    page_url = queue.get(block=False)
                except Empty:
                    break

                html = await fetch(session, page_url)
                # Run list parsing in the same thread (blocking)
                books = parse_books_list(html)

                if not books:
                    break

                # Create tasks to fetch details for each book found
                for book in books:
                    tg.create_task(fetch_book_with_details(session, book))

            all_books.extend(books)


def main(multithreaded: bool) -> None:
    queue = Queue()
    all_books = []
    # books.toscrape.com has 50 pages
    for page in range(1, 51):
        queue.put(BASE_URL.format(page))
    
    start_time = perf_counter()
    if multithreaded:
        print("Using multithreading for fetching books...")
        workers: int = 8  # no of CPU cores to use
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for _ in range(workers):
                executor.submit(lambda: asyncio.run(worker(queue, all_books)))
    else:
        print("Using single thread (blocking parsing)...")
        asyncio.run(worker(queue, all_books))

    end_time = perf_counter()

    # Total books should be 50 pages * 20 books/page = 1000
    total_books = len(all_books)
    print(
        f"\nScraped {total_books} books. "
        f"Total time: {end_time - start_time:.2f}s. "
        f"Speed: {total_books / (end_time - start_time):.0f} books/sec"
    )


if __name__ == "__main__":
    parser = ArgumentParser(description="Scrape books.toscrape.com books and details.")
    parser.add_argument(
        "--multithreaded",
        action="store_true",
        default=False,
        help="Use multithreading for fetching books.",
    )
    args = parser.parse_args()
    main(args.multithreaded)
