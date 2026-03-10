import concurrent.futures
import time
import psycopg2
from psycopg2 import sql
from database import localConnection
import importlib
import random
import psycopg2.extras
import sys
import os
from datetime import datetime
from  fetchcanocaialLink import process_urls_in_batches
import tldextract
import re
import asyncio


class DualLogger:
    def __init__(self):
        # Define base directories
        home_dir = os.path.expanduser('~')  # Get the home directory
        logs_dir = os.path.join(home_dir, 'log')  # Define the logs directory
        # Ensure logs directory exists
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Generate log file name based on the current date
        today = datetime.now().strftime('%Y%m%d')
        self.file_name = os.path.join(logs_dir, f'Google2Cbtrack_{today}.log')

    def write(self, message):
        # Write message to file with UTF-8 encoding
        with open(self.file_name, 'a', encoding='utf-8') as f:
            f.write(message)
        # Write message to console
        #sys.__stdout__.write(message)
        #sys.__stdout__.flush()

    def flush(self):
        sys.__stdout__.flush()

# Initialize DualLogger
# sys.stdout = DualLogger()

stdlib_fnmatch = importlib.import_module('fnmatch')

def getMainDomainName(url):
    extracted = tldextract.extract(url)
    publication = "{}.{}".format(extracted.domain, extracted.suffix)
    return publication


def process_batch(batch_size):
    while True:
        conn = localConnection()
        cursor = conn.cursor()

        try:
            # Get a unique COUNTER_EXTRACT number
            counter_extract_value = random.randint(1, 10000000)
            cursor.execute(
                sql.SQL('''
                    UPDATE "GOOGLE_SEARCH_LINK_BTRACK"
                    SET "COUNTER_EXTRACT" = %s
                    WHERE ctid IN (
                    SELECT ctid
                    FROM "GOOGLE_SEARCH_LINK_BTRACK"
                    
                    WHERE "COUNTER_EXTRACT" IS NULL
                AND "CANONICAL_LINK" IS NULL
                AND ("ISERROR" = B'0' OR "ISERROR" IS NULL)
                AND "PUBLICATION" <> 'msn.com'
				and "KEYWORD" = 'bTrackCBMSID'
                    ORDER BY "ARTICLEDATE" DESC
                    
                    FOR UPDATE
                    LIMIT %s
                )
                RETURNING "LINK"
                '''),
                [counter_extract_value, batch_size]
            )
            updated_links = cursor.fetchall()
            conn.commit()

            if not updated_links:
                print("No more links to process.")
                break

            for rss_url, in updated_links:
                try:
                    cursor.execute(
                        sql.SQL('''
                            SELECT "LINK", "PUBLICATION", "ARTICLEDATE", "KEYWORD"
                            FROM public."GOOGLE_SEARCH_LINK_BTRACK"
                            WHERE "LINK" = %s ORDER BY "ARTICLEDATE" DESC
                        '''),
                        [rss_url]
                    )
                    row = cursor.fetchone()
                    if row:
                        rss_url, publication, article_date, keyword = row
                        if keyword is None:
                            keyword = ' '
                        urls = [ {"rss":rss_url , "publication" : publication }]
                        original_link = asyncio.run(process_urls_in_batches(urls))
                        
                        publication_rss = getMainDomainName(original_link)
                        print(publication_rss, publication)
                        if re.search(publication_rss, publication) and not original_link == 'https://www.msn.com/en-ca':
                            formatted_values = []
                            source = 'GoogleBTrack'
                            processed = None
                            process_status = 'New'
                            process_batch = None
                            insert_query = """
                            INSERT INTO public."ALL_SEARCH_LINK_BTRACK" (
                                "LINK", "SOURCE", "PUBLICATION", "ARTICLEDATE", "KEYWORD", "PROCESSED", "PROCESSSTATUS", "PROCESSBATCH"
                            ) VALUES %s
                            ON CONFLICT ("LINK") DO NOTHING;
                            """
                            formatted_values.append((original_link, source, publication, article_date, keyword, processed, process_status, process_batch))

                            psycopg2.extras.execute_values(cursor, insert_query, formatted_values)
                            conn.commit()

                            cursor.execute(
                                sql.SQL('''
                                    UPDATE public."GOOGLE_SEARCH_LINK_BTRACK"
                                    SET "CANONICAL_LINK" = %s, "ISERROR" = B'0', "COUNTER_EXTRACT" = NULL
                                    WHERE "LINK" = %s
                                '''),
                                [original_link, rss_url]
                                
                            )
                            print(f"Successfully : Processed {original_link}")
                        else:
                            cursor.execute(
                                sql.SQL('''
                                    UPDATE public."GOOGLE_SEARCH_LINK_BTRACK"
                                    SET "ISERROR" = B'1', "COUNTER_EXTRACT" = NULL
                                    WHERE "LINK" = %s
                                '''),
                                [rss_url]
                            )
                            print(f"Error processing {rss_url}, no canonical link found")

                        conn.commit()
                        

                except Exception as e:
                    print(f"Error processing {rss_url}: {e}")
                    cursor.execute(
                        sql.SQL('''
                            UPDATE public."GOOGLE_SEARCH_LINK_BTRACK"
                            SET "ISERROR" = B'1', "COUNTER_EXTRACT" = NULL
                            WHERE "LINK" = %s
                        '''),
                        [rss_url]
                    )
                    conn.commit()

        finally:
            cursor.close()
            conn.close()
    # driver.quit()

def get_original_links(num_processes=50, batch_size=50): #num_process should be in env
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = []
        for _ in range(num_processes):
            # Submit the batch processing with a delay between submissions
            future = executor.submit(process_batch, batch_size)
            futures.append(future)
            time.sleep(20)  # Delay between submissions

        concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

if __name__ == "__main__":
    num_processes = int(os.getenv("BTRACK_NUM_PROCESSES", 1))  # fallback to 1
    batch_size = int(os.getenv("BTRACK_BATCH_SIZE", 50))        # fallback to 50
    get_original_links(num_processes=num_processes, batch_size=batch_size) # Adjust num_processes and batch_size as needed
