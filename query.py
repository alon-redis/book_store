import argparse
import redis
import json
import random
import time
import logging
import threading
from faker import Faker
from concurrent.futures import ThreadPoolExecutor

import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path  
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.query import NumericFilter, Query
from redis.commands.search.aggregation import AggregateRequest

# Define constants
REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"
INDEX_NAME = "idx:books"

fake = Faker()

#def make_key(book_id):
#    return f"{REDIS_KEY_BASE}:{book_id}"

def create_redis_connection_pool(redis_url, max_connections):
    return redis.ConnectionPool.from_url(redis_url, max_connections=max_connections)

def index_exists(r, index_name):
    try:
        index_info = r.ft(index_name).info()
        return True
    except redis.exceptions.ResponseError as e:
        if "Unknown Index name" in str(e):
            return False
        else:
            raise

def execute_queries(r, duration):
    start_time = time.time()
    commands_executed = 0
    while (time.time() - start_time) < duration:
        commands = [
        lambda: r.pexpire("alon:shmuely:redis:data:store:application:" + str(random.randint(1, 100)), 2),
        lambda: r.ft(INDEX_NAME).search(Query("green").return_field("$.description")),
        lambda: r.delete("alon:shmuely:redis:data:store:application:" + str(random.randint(1, 100))),
        lambda: r.ft(INDEX_NAME).search(Query("@geo:[34.0060 40.7128 500 km]")),
        lambda: r.ft(INDEX_NAME).aggregate(AggregateRequest("@geo:[-73.982254 40.753181 1000 km]").load("@geo").apply(geodistance="geodistance(@geo, -73.982254, 40.753181)")),
        lambda: r.ft(INDEX_NAME).aggregate(aggregations.AggregateRequest("*").group_by([], reducers.count().alias("total"))),
        lambda: r.ft(INDEX_NAME).search(Query(fake.word()).return_field("$.description", as_field="author")),
        lambda: r.ft(INDEX_NAME).search(Query("*").paging(0, 500).sort_by("year_published", asc=False)),
        lambda: r.ft(INDEX_NAME).search(Query("@year_published:[1948 1975]").return_field("$.description")),
        lambda: r.ft(INDEX_NAME).aggregate(aggregations.AggregateRequest(fake.word()).sort_by("@weight_grams")),
        lambda: r.ft(INDEX_NAME).search(Query(fake.word()).add_filter(NumericFilter("rating_votes", 900, 1000)))
        ]
        try:
            random.choice(commands)()
            commands_executed += 1
        except redis.exceptions.ResponseError as e:
            logging.error(f"Redis ResponseError during command execution: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
    logging.info(f"Random commands executed: {commands_executed}")

def main():
    arg_parser = argparse.ArgumentParser(description="Running the book store application")
    arg_parser.add_argument("--redis-url", default="redis://localhost:6379", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=500, type=int, help="Maximum number of Redis connections to accommodate all workers.")
    arg_parser.add_argument("--duration", default=60, type=int, help="Duration to run the random commands (in seconds)")
    arg_parser.add_argument("--workers", default=50, type=int, help="Number of concurrent workers")
    args = arg_parser.parse_args()

    redis_pool = create_redis_connection_pool(args.redis_url, args.max_connections)
    r = redis.Redis(connection_pool=redis_pool)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(execute_queries, r, args.duration) for _ in range(args.workers)]
        for future in futures:
            try:
                future.result()  # Waiting for all threads to complete
            except Exception as e:
                logging.error(f"Error in thread execution: {e}")

if __name__ == "__main__":
    main()
