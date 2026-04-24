import argparse
import random
import threading
import time

import redis
from faker import Faker
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.query import Query

try:
    from redis.commands.search.index_definition import IndexDefinition, IndexType
except ImportError:
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType


REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"
INDEX_NAME = "idx:books:v2"
DATA_VERIFICATION_SUCCESSFUL = 0
DATA_VERIFICATION_ERROR = 0
SUCCESSFUL_WRITE = 0
UNSUCCESSFUL_WRITE = 0

fake = Faker()


def make_key(book_id):
    return f"{REDIS_KEY_BASE}:{book_id}"


def create_redis_connection_pool(redis_url, max_connections):
    return redis.ConnectionPool.from_url(redis_url, max_connections=max_connections)


def get_index_info(connection_pool, index_name):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        return r.ft(index_name).info()
    except redis.exceptions.ResponseError as e:
        if "Unknown Index name" in str(e):
            return None
        raise
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to check index existence. Error: {str(e)}")
        return None


def drop_search_index(connection_pool):
    r = redis.Redis(connection_pool=connection_pool)

    try:
        r.ft(INDEX_NAME).dropindex(delete_documents=False)
        print(f"Dropped search index '{INDEX_NAME}' without deleting hash documents.")
    except redis.exceptions.ResponseError as e:
        if "Unknown Index name" not in str(e):
            raise


def create_search_index(connection_pool, recreate=False):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        index_info = get_index_info(connection_pool, INDEX_NAME)

        if index_info and not recreate:
            print(f"Search index '{INDEX_NAME}' already exists.")
            return

        if recreate:
            drop_search_index(connection_pool)

        print("Creating search index.")
        r.ft(INDEX_NAME).create_index(
            [
                TextField("author", sortable=True),
                TagField("id", sortable=True),
                TextField("description", sortable=True),
                TagField("editions", separator=",", sortable=True),
                TagField("genres", separator=",", sortable=True),
                NumericField("pages", sortable=True),
                TextField("title", sortable=True),
                NumericField("year_published", sortable=True),
                NumericField("rating_votes", sortable=True),
                NumericField("score", sortable=True),
                TagField("status", separator=",", sortable=True),
                TagField("stock_id", separator=",", sortable=True),
                TagField("format", sortable=True),
                TagField("is_available", sortable=True),
                NumericField("price", sortable=True),
                TagField("isbn", sortable=True),
                GeoField("geo", sortable=True),
                TextField("publisher", sortable=True),
                TextField("book_series", sortable=True),
                TextField("main_character", sortable=True),
                TextField("location", sortable=True),
                TextField("address", sortable=True),
                NumericField("edition_number", sortable=True),
                NumericField("chapter_count", sortable=True),
                NumericField("review_count", sortable=True),
                NumericField("citation_count", sortable=True),
                NumericField("publishing_delay", sortable=True),
                NumericField("word_count", sortable=True),
                NumericField("timestamp", sortable=True),
                NumericField("reading_time_minutes", sortable=True),
                NumericField("global_sales", sortable=True),
                NumericField("translations_count", sortable=True),
                NumericField("author_age_at_publication", sortable=True),
                NumericField("weight_grams", sortable=True),
                NumericField("width_cm", sortable=True),
                NumericField("height_cm", sortable=True),
                NumericField("depth_cm", sortable=True),
            ],
            definition=IndexDefinition(
                index_type=IndexType.HASH,
                prefix=[f"{REDIS_KEY_BASE}:"]
            )
        )
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to create search index. Error: {str(e)}")


def format_tag_values(values):
    return ",".join(values)


def generate_random_book(book_id):
    inventory = [
        {
            "status": random.choice(["available", "maintenance", "on_loan", "for_sale"]),
            "stock_id": f"{book_id}_{num}",
        }
        for num in range(random.randint(1, 10))
    ]

    return {
        "author": fake.name(),
        "id": str(book_id),
        "description": fake.paragraph(random.randint(25, 80)),
        "editions": format_tag_values(
            random.sample(
                [
                    "english", "spanish", "french", "german", "italian", "chinese",
                    "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
                    "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
                    "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
                    "indonesian", "hungarian", "czech", "slovak", "romanian",
                    "bulgarian", "ukrainian", "serbian", "croatian", "slovenian",
                    "latvian",
                ],
                k=random.randint(1, 5),
            )
        ),
        "genres": format_tag_values(
            random.sample(
                [
                    "comics (superheroes)", "fiction", "non-fiction", "science fiction",
                    "fantasy", "mystery", "romance", "history", "horror", "biography",
                    "thriller", "self-help", "poetry", "cookbooks", "memoir",
                    "young adult", "children's literature", "drama", "travel",
                    "science", "art", "philosophy", "psychology", "religion",
                    "true crime", "graphic novel", "adventure", "political",
                    "health", "humor",
                ],
                k=random.randint(1, 6),
            )
        ),
        "pages": random.randint(50, 1500),
        "title": " ".join(fake.words(nb=random.randint(1, 5))),
        "year_published": random.randint(1900, 2023),
        "rating_votes": random.randint(1, 1000),
        "score": round(random.uniform(1, 5), 2),
        "status": format_tag_values([item["status"] for item in inventory]),
        "stock_id": format_tag_values([item["stock_id"] for item in inventory]),
        "format": random.choice(["hardcover", "paperback", "ebook"]),
        "is_available": random.choice(["true", "false"]),
        "price": round(random.uniform(5, 100), 2),
        "isbn": fake.isbn13(),
        "geo": f"{fake.longitude()},{fake.latitude()}",
        "publisher": fake.company(),
        "book_series": " ".join(fake.words(nb=random.randint(1, 3))),
        "main_character": fake.first_name(),
        "location": fake.city(),
        "address": fake.address().replace("\n", ", "),
        "edition_number": random.randint(1, 10),
        "chapter_count": random.randint(5, 50),
        "review_count": random.randint(0, 5000),
        "citation_count": random.randint(0, 1000),
        "publishing_delay": random.randint(-356, 1000),
        "word_count": random.randint(10000, 150000),
        "timestamp": int(fake.unix_time()),
        "reading_time_minutes": random.randint(30, 1200),
        "global_sales": random.randint(1000, 1000000),
        "translations_count": random.randint(1, 50),
        "author_age_at_publication": random.randint(20, 80),
        "weight_grams": random.randint(-100, 2000),
        "width_cm": round(random.uniform(10, 30), 2),
        "height_cm": round(random.uniform(20, 40), 2),
        "depth_cm": round(random.uniform(1, 10), 2),
    }


def write_book_hash(r, book_id, book_data):
    stringified_book = {field: str(value) for field, value in book_data.items()}
    return r.hset(make_key(book_id), mapping=stringified_book)


def print_live_status(stop_event):
    while not stop_event.is_set():
        print(
            f"\rCurrent Status - Successful Verification: {DATA_VERIFICATION_SUCCESSFUL}, "
            f"Error Verification: {DATA_VERIFICATION_ERROR}, "
            f"Successful Writes: {SUCCESSFUL_WRITE}, "
            f"Unsuccessful Writes: {UNSUCCESSFUL_WRITE}",
            end="",
            flush=True,
        )
        time.sleep(1)


def write_data_verification(connection_pool):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        book_data = generate_random_book(0)
        book_data["author"] = "Alon Shmuely"
        book_data["title"] = "QA architect"
        book_data["address"] = "98765 Ein Dor Apt. 0001 Rishon Lezion, IL 1948"
        write_book_hash(r, 0, book_data)
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to write data verification. Error: {str(e)}")


def read_data_verification(connection_pool, stop_event):
    global DATA_VERIFICATION_SUCCESSFUL
    global DATA_VERIFICATION_ERROR

    try:
        r = redis.Redis(connection_pool=connection_pool)
        while not stop_event.is_set():
            results = r.ft(INDEX_NAME).search(Query("Shmuely").return_field("title")).docs
            if results and getattr(results[0], "title", None) == "QA architect":
                DATA_VERIFICATION_SUCCESSFUL += 1
            else:
                DATA_VERIFICATION_ERROR += 1
    except (IndexError, redis.exceptions.ConnectionError) as e:
        print(f"Data verification failed. Error: {str(e)}")
        write_data_verification(connection_pool)


def generating_books(connection_pool, max_books, max_random):
    global SUCCESSFUL_WRITE
    global UNSUCCESSFUL_WRITE

    try:
        r = redis.Redis(connection_pool=connection_pool)
        for _ in range(1, max_books + 1):
            book_id = random.randint(1, max_random)
            book_data = generate_random_book(book_id)
            response = write_book_hash(r, book_id, book_data)
            if response:
                SUCCESSFUL_WRITE += 1
            else:
                UNSUCCESSFUL_WRITE += 1
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to generate books. Error: {str(e)}")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the hash-based book store application")
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=10, type=int, dest="max_connections", help="Maximum number of Redis connections.")
    arg_parser.add_argument("--max-books", default=3000, type=int, dest="max_books", help="Maximum number of books")
    arg_parser.add_argument("--max-random", default=3000, type=int, dest="max_random", help="Maximum random number of books")
    arg_parser.add_argument("--flush", action="store_true", help="Flush the Redis database on startup")
    arg_parser.add_argument("--recreate-index", action="store_true", help="Drop and recreate the search index without deleting hash documents.")
    args = arg_parser.parse_args()

    try:
        print(f"Connecting to Redis at {args.redis_url} with a max of {args.max_connections} connections")
        redis_pool = create_redis_connection_pool(args.redis_url, args.max_connections)

        if args.flush:
            print("Flushing Redis database...")
            r = redis.Redis(connection_pool=redis_pool)
            r.flushall()

        create_search_index(redis_pool, recreate=args.recreate_index)
        write_data_verification(redis_pool)

        stop_event = threading.Event()
        verification_thread = threading.Thread(target=read_data_verification, args=(redis_pool, stop_event))
        write_thread = threading.Thread(target=generating_books, args=(redis_pool, args.max_books, args.max_random))

        status_stop_event = threading.Event()
        status_thread = threading.Thread(target=print_live_status, args=(status_stop_event,))
        status_thread.start()

        verification_thread.start()
        write_thread.start()

        write_thread.join()
        stop_event.set()
        verification_thread.join()

        status_stop_event.set()
        status_thread.join()

        print(f"\n\n\nRun Summary")
        print(f"Successful verification: {DATA_VERIFICATION_SUCCESSFUL}\nError verification: {DATA_VERIFICATION_ERROR}")
        print(f"Successful writes: {SUCCESSFUL_WRITE}\nError writes: {UNSUCCESSFUL_WRITE}")

    except redis.exceptions.ConnectionError as e:
        print(f"Failed to connect to Redis. Error: {str(e)}")
