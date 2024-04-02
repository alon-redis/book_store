import argparse
import redis
import json
import random
import time
import threading
from faker import Faker

import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path  
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.query import NumericFilter, Query
from redis.commands.search.aggregation import AggregateRequest


# TO LIST
# add drop index and alter every X commands
# make alter command more heavy by using larger field than address (internal data 2?)
# enable the pexire once the bug solved
# complicated search commands 
# multi exec commands
# change modules configuration and threads (vertical scale)
# search by hash
# performance improvments
# pipeline
# add special charcters
# add support in cluster mode
# add support in resp3
# log the redis errors to buffer or file

# Define constants
REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"  
INDEX_NAME = "idx:books"
DATA_VERIFICATION_SUCCESSFUL = 0
DATA_VERIFICATION_ERROR = 0
RANDOM_CMD = 0 
SUCCESSFUL_WRITE = 0
UNSUCCESSFUL_WRITE = 0

fake = Faker()

def make_key(book_id):
    return f"{REDIS_KEY_BASE}:{book_id}"

def create_redis_connection_pool(redis_url, max_connections):
    return redis.ConnectionPool.from_url(redis_url, max_connections=max_connections)

def index_exists(connection_pool, index_name):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        index_info = r.ft(index_name).info()
        if index_info:
            print(f"Search index '{index_name}' already exists.")
            return True
        else:
            print(f"Search index '{index_name}' does not exist. Creating it now...")
            create_search_index(connection_pool)
            return True
    except redis.exceptions.ResponseError as e:
       print(f"Error: {str(e)}")
       return False
    except redis.exceptions.ConnectionError as e:
       print(f"Failed to check index existence. Error: {str(e)}")
       return False

def create_search_index(connection_pool):
    try:
        r = redis.Redis(connection_pool=connection_pool) 
        if not index_exists(connection_pool, INDEX_NAME):
            print("Creating search index.")
            r.ft(INDEX_NAME).create_index(
                [
                    TextField("$.author", as_name="author", sortable=True),
                    TagField("$.id", as_name="id", sortable=True),
                    TextField("$.description", as_name="description", sortable=True), 
                    TagField("$.editions[*]", as_name="editions", sortable=True),
                    TagField("$.genres[*]", as_name="genres", sortable=True),
                    NumericField("$.pages", as_name="pages", sortable=True),  
                    TextField("$.title", as_name="title", sortable=True),
                    NumericField("$.year_published", as_name="year_published", sortable=True),
                    NumericField("$.metrics.rating_votes", as_name="rating_votes", sortable=True),
                    NumericField("$.metrics.score", as_name="score", sortable=True),
                    TagField("$.inventory[*].status", as_name="status", sortable=True),
                    TagField("$.inventory[*].stock_id", as_name="stock_id", sortable=True),
                    TagField("$.format", as_name="format", sortable=True),
                    TagField("$.is_available", as_name="is_available", sortable=True), 
                    NumericField("$.price", as_name="price", sortable=True),
                    TagField("$.isbn", as_name="isbn", sortable=True), 
                    redis.commands.search.field.GeoField("$.geo", as_name="geo", sortable=True),
                    TextField("$.publisher", as_name="publisher", sortable=True),
                    TextField("$.book_series", as_name="book_series", sortable=True),
                    TextField("$.main_character", as_name="main_character", sortable=True),
                    TextField("$.location", as_name="location", sortable=True),
                    NumericField("$.edition_number", as_name="edition_number", sortable=True),
                    NumericField("$.chapter_count", as_name="chapter_count", sortable=True),
                    NumericField("$.review_count", as_name="review_count", sortable=True),
                    NumericField("$.citation_count", as_name="citation_count", sortable=True),
                    NumericField("$.publishing_delay", as_name="publishing_delay", sortable=True),
                    NumericField("$.word_count", as_name="word_count", sortable=True),
                    NumericField("$.timestamp", as_name="timestamp", sortable=True),                    
                    NumericField("$.reading_time_minutes", as_name="reading_time_minutes", sortable=True),
                    NumericField("$.global_sales", as_name="global_sales", sortable=True),
                    NumericField("$.translations_count", as_name="translations_count", sortable=True),
                    NumericField("$.author_age_at_publication", as_name="author_age_at_publication", sortable=True),
                    NumericField("$.weight_grams", as_name="weight_grams", sortable=True),
                    NumericField("$.dimensions.width_cm", as_name="width_cm", sortable=True), 
                    NumericField("$.dimensions.height_cm", as_name="height_cm", sortable=True),
                    NumericField("$.dimensions.depth_cm", as_name="depth_cm", sortable=True) 
                ],
                definition=IndexDefinition( 
                    index_type=IndexType.JSON,  
                    prefix=[f"{REDIS_KEY_BASE}:"]
                )
            )
        else:
            print("Search index already exists.")
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to create search index. Error: {str(e)}")

def generate_random_book(id):
    return {
        "author": fake.name(),
        "id": str(id),
        "description": fake.paragraph(random.randint(25, 80)),
        "editions": random.sample(["english", "spanish", "french", "german", "italian", "chinese", "japanese", "russian", "arabic", "portuguese", "korean", "dutch", "swedish", "norwegian", "danish", "finnish", "polish", "turkish", "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese", "indonesian","hungarian", "czech", "slovak", "romanian", "bulgarian", "ukrainian", "serbian", "croatian", "slovenian", "latvian"], k=random.randint(1, 5)),
        "genres": random.sample(["comics (superheroes)", "fiction", "non-fiction", "science fiction", "fantasy", "mystery", "romance", "history", "horror", "biography", "thriller", "self-help", "poetry", "cookbooks", "memoir", "young adult", "children's literature", "drama", "travel", "science", "art", "philosophy", "psychology", "religion", "true crime", "graphic novel", "adventure", "political", "health", "humor"], k=random.randint(1, 6)),
        "inventory": [
            {
                "status": random.choice(["available", "maintenance", "on_loan", "for_sale"]),
                "stock_id": f"{id}_{num}"
            } for num in range(random.randint(1, 10))
        ],
        "metrics": {
            "rating_votes": random.randint(1, 1000),
            "score": round(random.uniform(1, 5), 2)
        },
        "pages": random.randint(50, 1500),
        "title": " ".join(fake.words(nb=random.randint(1, 5))),
        "url": fake.url(),
        "year_published": random.randint(1900, 2023),
        "format": random.choice(["hardcover", "paperback", "ebook"]),
        "is_available": random.choice([True, False]),
        "price": round(random.uniform(5, 100), 2),
        "isbn": fake.isbn13(),
        "address": fake.address(),
        "geo": f"{fake.longitude()}, {fake.latitude()}",
        "weight_grams": random.randint(-100, 2000),
        "dimensions": {
            "width_cm": round(random.uniform(10, 30), 2),
            "height_cm": round(random.uniform(20, 40), 2),
            "depth_cm": round(random.uniform(1, 10), 2)
        },
        "edition_number": random.randint(1, 10),
        "chapter_count": random.randint(5, 50),
        "review_count": random.randint(0, 5000),
        "citation_count": random.randint(0, 1000),
        "timestamp": fake.unix_time(),
        "publishing_delay": random.randint(-356, 1000),
        "word_count": random.randint(10000, 150000),
        "reading_time_minutes": random.randint(30, 1200),
        "global_sales": random.randint(1000, 1000000),
        "translations_count": random.randint(1, 50),
        "publisher": fake.company(),
        "main_character": fake.first_name(),
        "location": fake.city(),
        "author_age_at_publication": random.randint(20, 80),
        "alon": random.randint(-10000, 10000)
    }  
    
def print_live_status(stop_event):
   while not stop_event.is_set():
       print(f"\rCurrent Status - Successful Verification: {DATA_VERIFICATION_SUCCESSFUL}, "
              f"Error Verification: {DATA_VERIFICATION_ERROR}, "
              f"Random Commands: {RANDOM_CMD}, "
              f"Successful Writes: {SUCCESSFUL_WRITE}, "
              f"Unsuccessful Writes: {UNSUCCESSFUL_WRITE}", end='', flush=True)
       time.sleep(1) 
       
def write_data_verification(connection_pool):
   try:
       r = redis.Redis(connection_pool=connection_pool)
       book_data = generate_random_book(0)
       book_data['author'] = 'Alon Shmuely'
       book_data['title'] = 'QA architect'
       book_data['address'] = '98765 Ein Dor Apt. 0001 Rishon Lezion, IL 1948'
       r.json().set("alon:shmuely:redis:data:store:application:" + str(0), Path.root_path(), book_data)
   except redis.exceptions.ConnectionError as e:
       print(f"Failed to write data verification. Error: {str(e)}")
       
def read_data_verification(connection_pool, stop_event):
   global DATA_VERIFICATION_SUCCESSFUL 
   global DATA_VERIFICATION_ERROR
   try:
      r = redis.Redis(connection_pool=connection_pool)
      while not stop_event.is_set():
          R2 = r.ft(INDEX_NAME).search(Query("Shmuely").return_field("$.title")).docs
          if R2 and R2[0]['$.title'] == "QA architect":
              DATA_VERIFICATION_SUCCESSFUL += 1
          else:
              DATA_VERIFICATION_ERROR += 1
   except (IndexError, redis.exceptions.ConnectionError) as e:
      print(f"Data verification failed. Error: {str(e)}")
      write_data_verification()
      
def random_commands(connection_pool, stop_event):
    global RANDOM_CMD
    try:
        r4 = redis.Redis(connection_pool=connection_pool)
        alias_name = f"{INDEX_NAME}_alias"
        exec('try: r4.ft(INDEX_NAME).aliasadd(alias_name) \nexcept: pass')
        # FT.ALTER command and verification
        exec('try: r4.ft(INDEX_NAME).alter_schema_add(TextField("$.address", as_name="address")) \nexcept: pass')
        r4.ft(INDEX_NAME).search(Query('@address:Dor').return_field("$.description", as_field="author")).docs

        # Define a new command to alter the index with a random field
        def alter_index_with_random_field():
            try:
                # Randomly decide a field name and type
                field_name = f"random_field_{random.randint(1, 1000)}"
                field_type = random.choice([TextField, NumericField, TagField])
                new_field = field_type(f"$.{field_name}", as_name=field_name)

                # Adding the new field to the index
                r4.ft(INDEX_NAME).alter_schema_add(new_field)
            except redis.exceptions.ResponseError as e:
                print(f"Error altering index with random field: {str(e)}")


        commands = [
#         lambda: r4.ft(INDEX_NAME).dropindex(delete_documents=False),
#         lambda: r4.pexpire("alon:shmuely:redis:data:store:application:" + str(random.randint(1, 100)), 2)
          alter_index_with_random_field,
          lambda: r4.ft(alias_name).search(Query("green").return_field("$.description")),
          lambda: r4.delete("alon:shmuely:redis:data:store:application:" + str(random.randint(1, 100))),
          lambda: r4.ft(INDEX_NAME).search(Query("@geo:[34.0060 40.7128 500 km]")),
          lambda: r4.ft(INDEX_NAME).aggregate(AggregateRequest("@geo:[-73.982254 40.753181 1000 km]").load("@geo").apply(geodistance="geodistance(@geo, -73.982254, 40.753181)")).rows,
          lambda: r4.ft(INDEX_NAME).aggregate(aggregations.AggregateRequest("*").group_by([], reducers.count().alias("total"))).rows,
          lambda: r4.ft(INDEX_NAME).search(Query(fake.word()).return_field("$.description", as_field="author")).docs,
          lambda: r4.ft(INDEX_NAME).search(Query("*").paging(0, 500).sort_by("year_published", asc=False)),
          lambda: r4.ft(INDEX_NAME).search(Query("@year_published:[1948 1975]").return_field("$.description", as_field="description")).docs,
          lambda: r4.ft(INDEX_NAME).aggregate(aggregations.AggregateRequest(fake.word()).sort_by("@weight_grams")).rows,
          lambda: r4.ft(INDEX_NAME).search(Query(fake.word()).add_filter(NumericFilter("rating_votes", 900, 1000)))
        ]
        while not stop_event.is_set():
          random_command = random.choice(commands)
          random_command()  

          RANDOM_CMD += 1
    except (IndexError, redis.exceptions.ConnectionError) as e:
        print(f"random command request failed. Error: {str(e)}")
        
def generating_books(connection_pool, max_books, max_random):
    global SUCCESSFUL_WRITE 
    global UNSUCCESSFUL_WRITE
    try:
        r = redis.Redis(connection_pool=connection_pool)
        for x in range(1, max_books + 1):
            book_id = random.randint(1, max_random)
            book_data = generate_random_book(book_id)
            response = r.json().set("alon:shmuely:redis:data:store:application:" + str(book_id), Path.root_path(), book_data)
            if response:
              SUCCESSFUL_WRITE += 1
            else:
              UNSUCCESSFUL_WRITE += 1
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to generate books. Error: {str(e)}")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the book store application")
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=10, type=int, dest="max_connections", help="Maximum number of Redis connections.")
    arg_parser.add_argument("--max-books", default=3000, type=int, dest="max_books", help="Maximum number of books")
    arg_parser.add_argument("--max-random", default=3000, type=int, dest="max_random", help="Maximum random number of books")
    arg_parser.add_argument("--flush", action='store_true', help="Flush the Redis database on startup")
    arg_parser.add_argument("--run-random-cmds", action='store_true', help="Run random commands thread")
    args = arg_parser.parse_args()

    try:
        print(f"Connecting to Redis at {args.redis_url} with a max of {args.max_connections} connections")
        redis_pool = create_redis_connection_pool(args.redis_url, args.max_connections)

        if args.flush:
            print("Flushing Redis database...")
            r = redis.Redis(connection_pool=redis_pool)
            r.flushall()

        create_search_index(redis_pool)
        write_data_verification(redis_pool)

        # Create thread objects
        stop_event = threading.Event()  
        verification_thread = threading.Thread(target=read_data_verification, args=(redis_pool, stop_event))
        write_thread = threading.Thread(target=generating_books, args=(redis_pool, args.max_books, args.max_random))
        chaos_thread = None
        if args.run_random_cmds:
            chaos_thread = threading.Thread(target=random_commands, args=(redis_pool, stop_event))

        # Create a stop event for the live status thread
        status_stop_event = threading.Event()

        # Create and start the live status thread
        status_thread = threading.Thread(target=print_live_status, args=(status_stop_event,))
        status_thread.start()
        
        # Start the main threads
        verification_thread.start()
        write_thread.start()
        if args.run_random_cmds:
            chaos_thread.start()

        write_thread.join()
        stop_event.set() 
        verification_thread.join()
        if args.run_random_cmds:
            chaos_thread.join()

        # Stop the live status thread and wait for it to finish
        status_stop_event.set() 
        status_thread.join()

        print(f"\n\n\nRun Summary")
        print(f"Successful verification: {DATA_VERIFICATION_SUCCESSFUL}\nError verification: {DATA_VERIFICATION_ERROR}")
        print(f"Random commands: {RANDOM_CMD}")
        print(f"Successful writes: {SUCCESSFUL_WRITE}\nError writes: {UNSUCCESSFUL_WRITE}")

    except redis.exceptions.ConnectionError as e:
        print(f"Failed to connect to Redis. Error: {str(e)}")
