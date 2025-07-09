from flask import Flask, request, jsonify, render_template, Response, stream_with_context
import requests
import traceback
import orjson as json
import time
import random
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import sqlite3
import threading
import uuid
from datetime import datetime
from contextlib import contextmanager

import gc

MAX_WORKERS = 8  # Increased for faster processing
BATCH_SIZE = 25  # Larger batches for better throughput
REQUEST_TIMEOUT = 10  # Reduced timeout for faster failures
GC_ENABLED = False  # Disable frequent GC for speed

app = Flask(__name__, static_folder="static", template_folder="templates")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure the maximum number of concurrent API requests
MAX_WORKERS = 5
# Configure batch size for large requests
BATCH_SIZE = 20

# Database setup
DATABASE = 'voila_search.db'
db_lock = threading.Lock()

# Create a session for HTTP requests reuse
http_session = requests.Session()
# Configure session for better performance
http_session.mount('https://', requests.adapters.HTTPAdapter(
    pool_connections=10,
    pool_maxsize=20,
    max_retries=1
))

def get_db_connection():
    """Get a database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize the database with required tables"""
    with get_db_connection() as conn:
        # Enable WAL mode and optimize SQLite settings
        conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=10000;
            PRAGMA temp_store=memory;
        """)

        # Create search sessions table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS search_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                search_terms TEXT NOT NULL,
                search_type TEXT NOT NULL,
                region_info TEXT,
                status TEXT DEFAULT 'processing',
                total_terms INTEGER DEFAULT 0,
                processed_terms INTEGER DEFAULT 0,
                duplicate_count INTEGER DEFAULT 0,
                duplicates TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create search results table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS search_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_session_id TEXT NOT NULL,
                search_term TEXT NOT NULL,
                product_data TEXT NOT NULL,
                total_found INTEGER DEFAULT 0,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (search_session_id) REFERENCES search_sessions (session_id)
            )
        ''')

        # Create index for faster queries
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_results_session 
            ON search_results(search_session_id)
        ''')

        conn.commit()

# Initialize database on startup
init_database()

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('index.html')

sys.setrecursionlimit(2000)  # Default is usually 1000


def get_region_info(session_id):
    """
    Fetch the store / region details for the provided Voila `global_sid`
    **without** holding a long‑lived session in memory.

    A short‑lived `requests.Session()` is used only so any Set‑Cookie headers
    the server sends on *this* call are applied automatically to the *same*
    call’s redirect chain.  One request is enough: the response already
    contains `defaultCheckoutGroup.delivery.addressDetails`.
    """
    url = "https://voila.ca/api/cart/v1/carts/active"
    headers = {
        "accept": "application/json; charset=utf-8",
        "client-route-id": "d55f7f13-4217-4320-907e-eadd09051a7c",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    base_cookies = {"global_sid": session_id}

    try:
        # One‑shot session so redirects / Set‑Cookie work, but nothing is kept
        with requests.Session() as s:
            resp = s.get(url, headers=headers, cookies=base_cookies, timeout=15)
            resp.raise_for_status()

        try:
            data = resp.json()
        except ValueError:
            # Server returned non‑JSON; fall back to regex extraction
            return fallback_region_extraction(resp.text)

        # ── Extract fields ───────────────────────────────────────────────────
        region_info = {
            "regionId": data.get("regionId") or "unknown",
            "nickname": None,
            "displayAddress": None,
            "postalCode": None
        }

        delivery = (
            data.get("defaultCheckoutGroup", {})
                .get("delivery", {})
                .get("addressDetails", {})
        )
        region_info["nickname"]        = delivery.get("nickname")
        region_info["displayAddress"]  = delivery.get("displayAddress")
        region_info["postalCode"]      = delivery.get("postalCode")

        # Fallback nickname if API didn’t send addressDetails
        if not region_info["nickname"] and region_info["regionId"] != "unknown":
            region_info["nickname"] = f"Region {region_info['regionId']}"

        return region_info

    except requests.exceptions.Timeout:
        return {
            "regionId": "unknown",
            "nickname": "Timeout",
            "displayAddress": "API request timed out",
            "postalCode": "Unknown"
        }
    except RecursionError:
        return {
            "regionId": "unknown",
            "nickname": "Processing Error",
            "displayAddress": "Data too complex to process",
            "postalCode": "Unknown"
        }
    except Exception as exc:
        logging.error("get_region_info error: %s", exc)
        return {
            "regionId": "unknown",
            "nickname": "Error",
            "displayAddress": str(exc)[:50],
            "postalCode": "Unknown"
        }


def fallback_region_extraction(text_response):
    """Extract region info using regex as a fallback method"""
    # Initialize the region info with default values
    region_info = {
        "regionId": None,
        "nickname": None,
        "displayAddress": None,
        "postalCode": None
    }

    # Extract region ID
    region_id_match = re.search(r'"regionId"\s*:\s*"?(\d+)"?', text_response)
    if region_id_match:
        region_info["regionId"] = region_id_match.group(1)

    # Extract nickname
    nickname_match = re.search(r'"nickname"\s*:\s*"([^"]+)"', text_response)
    if nickname_match:
        region_info["nickname"] = nickname_match.group(1)

    # Extract display address
    addr_match = re.search(r'"displayAddress"\s*:\s*"([^"]+)"', text_response)
    if addr_match:
        region_info["displayAddress"] = addr_match.group(1)

    # Extract postal code
    postal_match = re.search(r'"postalCode"\s*:\s*"([^"]+)"', text_response)
    if postal_match:
        region_info["postalCode"] = postal_match.group(1)

    # If we couldn't find the region ID directly, try an alternative approach
    if not region_info["regionId"]:
        alt_region_match = re.search(r'"region"\s*:\s*{\s*"id"\s*:\s*"?(\d+)"?', text_response)
        if alt_region_match:
            region_info["regionId"] = alt_region_match.group(1)

    # Set a default nickname if none was found
    if not region_info["nickname"] and region_info["regionId"]:
        region_info["nickname"] = f"Region {region_info['regionId']}"

    return region_info

def parse_search_terms(search_input):
    """
    Parse search input into individual search terms.
    Handles comma-separated, newline-separated, and space-separated inputs.
    Also handles EA-code pattern recognition.
    Returns tuple of (unique_terms, duplicate_count, contains_ea_codes)
    """
    # Initialize variables to track duplicates and EA codes
    contains_ea_codes = False

    # First check for continuous EA codes and separate them
    if 'EA' in search_input:
        # This regex matches patterns of digits followed by 'EA'
        continuous_ea_pattern = r'(\d+EA)'
        # Replace with the same but with a space after
        search_input = re.sub(continuous_ea_pattern, r'\1 ', search_input)
        contains_ea_codes = True

    # Now try comma or newline separation
    terms = []
    if ',' in search_input or '\n' in search_input:
        # Split by commas and newlines
        terms = re.split(r'[,\n]', search_input)
    else:
        # Check for EA product codes pattern
        ea_codes = re.findall(r'\b\d+EA\b', search_input)
        if ea_codes:
            # If we found EA codes, use them
            terms = ea_codes
            contains_ea_codes = True
        else:
            # Otherwise, try splitting by spaces if the input is particularly long
            if len(search_input) > 50 and ' ' in search_input:
                terms = search_input.split()
            else:
                # Use the entire input as a single term
                terms = [search_input]

    # Clean up terms
    terms = [term.strip() for term in terms if term.strip()]

    # Count total terms before deduplication
    total_terms = len(terms)

    # Remove duplicates while preserving order
    seen = set()
    unique_terms = []
    duplicates = []

    for term in terms:
        if term not in seen:
            seen.add(term)
            unique_terms.append(term)
        else:
            duplicates.append(term)

    # Calculate how many duplicates were removed
    duplicate_count = total_terms - len(unique_terms)

    return unique_terms, duplicate_count, contains_ea_codes, duplicates

def fetch_product_data(product_id, session_id):
    """Fetch product data from Voila.ca API using the provided session ID"""
    try:
        url = "https://voila.ca/api/v6/products/search"

        headers = {
            "accept": "application/json; charset=utf-8",
            "client-route-id": "5fa0016c-9764-4e09-9738-12c33fb47fc2",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        cookies = {
            "global_sid": session_id
        }

        params = {
            "term": product_id
        }

        # Add timeout to prevent hanging requests
        response = http_session.get(url, headers=headers, params=params, cookies=cookies, timeout=REQUEST_TIMEOUT)

        if response.status_code != 200:
            print(f"API returned status code {response.status_code} for term {product_id}")
            return None

        # Use text response to avoid full JSON parsing
        text_response = response.text

        # Quick check for products
        if '"productId"' not in text_response and '"retailerProductId"' not in text_response:
            print(f"No products found for term {product_id}")
            return {"entities": {"product": {}}}

        # Create basic result structure
        result = {
            "entities": {
                "product": {}
            }
        }

        # Extract product IDs
        product_ids = []
        product_id_matches = re.finditer(r'"productId"\s*:\s*"([^"]+)"', text_response)
        for match in product_id_matches:
            product_ids.append(match.group(1))

        # Look for retailer product IDs if no direct product IDs
        if not product_ids:
            retailer_id_matches = re.finditer(r'"retailerProductId"\s*:\s*"([^"]+)"', text_response)
            for match in retailer_id_matches:
                product_ids.append("retailer_" + match.group(1))

        # Process each product ID
        for prod_id in product_ids:
            # Find where this product ID is mentioned
            search_pattern = f'"productId"\\s*:\\s*"{prod_id}"' if not prod_id.startswith("retailer_") else f'"retailerProductId"\\s*:\\s*"{prod_id[9:]}"'
            id_match = re.search(search_pattern, text_response)

            if id_match:
                # Find the containing object
                obj_start = text_response.rfind("{", 0, id_match.start())
                if obj_start >= 0:
                    # Find closing brace by counting nesting
                    brace_count = 1
                    obj_end = obj_start + 1

                    while brace_count > 0 and obj_end < len(text_response):
                        if text_response[obj_end] == "{":
                            brace_count += 1
                        elif text_response[obj_end] == "}":
                            brace_count -= 1
                        obj_end += 1

                    if brace_count == 0:
                        # Extract the product JSON
                        product_json = text_response[obj_start:obj_end]

                        try:
                            # Parse just this product object
                            import json
                            product_data = json.loads(product_json)

                            # Add to our result
                            actual_id = prod_id if not prod_id.startswith("retailer_") else product_data.get("productId", prod_id)
                            result["entities"]["product"][actual_id] = product_data
                        except json.JSONDecodeError as e:
                            print(f"Error parsing product JSON for {prod_id}: {str(e)}")
                            # Try fallback extraction
                            fallback_product = extract_product_fields(product_json, prod_id)
                            if fallback_product:
                                result["entities"]["product"][prod_id] = fallback_product

        # Create minimal entries if parsing failed
        if not result["entities"]["product"] and product_ids:
            print(f"Warning: Found {len(product_ids)} product IDs but couldn't parse them properly")
            for prod_id in product_ids:
                clean_id = prod_id[9:] if prod_id.startswith("retailer_") else prod_id
                result["entities"]["product"][clean_id] = {
                    "productId": clean_id,
                    "retailerProductId": product_id,
                    "name": f"Product {clean_id}",
                    "available": True
                }

        return result

    except requests.exceptions.Timeout:
        print(f"Request timeout for term {product_id}")
        return None
    except RecursionError:
        print(f"Recursion error fetching product data for {product_id}")
        return {"entities": {"product": {}}}
    except Exception as e:
        print(f"Unexpected error fetching product data for {product_id}: {str(e)}")
        return None


def extract_product_fields(product_json, product_id):
    """Extract essential product fields using regex when JSON parsing fails"""
    try:
        # Clean the product ID if it's a retailer ID
        clean_id = product_id[9:] if product_id.startswith("retailer_") else product_id

        # Create a basic product
        product = {
            "productId": clean_id,
            "retailerProductId": None,
            "name": None,
            "available": True,
            "brand": None,
            "categoryPath": [],
            "price": {
                "current": {
                    "amount": None,
                    "currency": "CAD"
                }
            }
        }

        # Extract retailerProductId - be careful with quotes and special characters
        retailer_id_match = re.search(r'"retailerProductId"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', product_json)
        if retailer_id_match:
            product["retailerProductId"] = retailer_id_match.group(1).replace('\\"', '"')

        # Extract name with better handling of escaped quotes and special characters
        name_match = re.search(r'"name"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', product_json)
        if name_match:
            product["name"] = name_match.group(1).replace('\\"', '"').replace('\\\\', '\\')

        # Extract brand with improved pattern
        brand_match = re.search(r'"brand"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', product_json)
        if brand_match:
            product["brand"] = brand_match.group(1).replace('\\"', '"').replace('\\\\', '\\')

        # Extract availability with more precise pattern
        available_match = re.search(r'"available"\s*:\s*(true|false)', product_json)
        if available_match:
            product["available"] = available_match.group(1) == "true"

        # Extract price with enhanced pattern
        price_match = re.search(r'"current"\s*:\s*{\s*"amount"\s*:\s*"([^"]+)"', product_json)
        if price_match:
            product["price"]["current"]["amount"] = price_match.group(1)

        # Extract image URL with better pattern
        image_match = re.search(r'"src"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', product_json)
        if image_match:
            product["image"] = {"src": image_match.group(1).replace('\\"', '"').replace('\\\\', '\\')}

        return product
    except Exception as e:
        print(f"Error in fallback extraction: {str(e)}")
        return None


def extract_product_info(product, search_term=None):
    """Extract product info in a memory-efficient way"""
    # Extract basic product details
    product_info = {
        "found": True,
        "searchTerm": search_term,
        "productId": product.get("productId"),
        "retailerProductId": product.get("retailerProductId"),
        "name": product.get("name"),
        "brand": product.get("brand"),
        "available": product.get("available", False),
        "imageUrl": None,
        "currency": "CAD"
    }

    # Safely extract image URL
    if "image" in product and isinstance(product["image"], dict):
        product_info["imageUrl"] = product["image"].get("src")

    # Safely extract category
    if "categoryPath" in product and isinstance(product["categoryPath"], list):
        product_info["category"] = " > ".join(product["categoryPath"])
    else:
        product_info["category"] = ""

    # Handle price information
    if "price" in product and isinstance(product["price"], dict):
        price_info = product["price"]

        # Current price
        if "current" in price_info and isinstance(price_info["current"], dict):
            product_info["currentPrice"] = price_info["current"].get("amount")
            product_info["currency"] = price_info["current"].get("currency", "CAD")

        # Original price
        if "original" in price_info and isinstance(price_info["original"], dict):
            product_info["originalPrice"] = price_info["original"].get("amount")

            # Calculate discount percentage
            if ("currentPrice" in product_info and "originalPrice" in product_info and
                product_info["currentPrice"] is not None and product_info["originalPrice"] is not None):
                try:
                    current_price = float(product_info["currentPrice"])
                    original_price = float(product_info["originalPrice"])

                    if original_price > current_price:
                        discount = ((original_price - current_price) / original_price * 100)
                        product_info["discountPercentage"] = round(discount)
                except (ValueError, TypeError):
                    pass

        # Unit price
        if "unit" in price_info and isinstance(price_info["unit"], dict):
            if "current" in price_info["unit"] and isinstance(price_info["unit"]["current"], dict):
                product_info["unitPrice"] = price_info["unit"]["current"].get("amount")
            product_info["unitLabel"] = price_info["unit"].get("label")

    # Extract offers, limiting to 5 to save memory
    if "offers" in product and isinstance(product["offers"], list):
        offers = product.get("offers", [])
        product_info["offers"] = offers[:5] if offers else []

    if "offer" in product:
        product_info["primaryOffer"] = product.get("offer")

    return product_info

def store_search_result(search_session_id, search_term, product_data, total_found):
    """Store a search result in the database"""
    with db_lock:
        with get_db_connection() as conn:
            conn.execute('''
                INSERT INTO search_results (search_session_id, search_term, product_data, total_found)
                VALUES (?, ?, ?, ?)
            ''', (search_session_id, search_term, json.dumps(product_data).decode(), total_found))
            conn.commit()

def store_search_results_batch(results_batch):
    """Store multiple search results in a single transaction for better performance"""
    if not results_batch:
        return

    with db_lock:
        with get_db_connection() as conn:
            conn.executemany('''
                INSERT INTO search_results (search_session_id, search_term, product_data, total_found)
                VALUES (?, ?, ?, ?)
            ''', results_batch)
            conn.commit()

def update_search_session_progress(search_session_id):
    """Update the processed_terms count for a search session"""
    with db_lock:
        with get_db_connection() as conn:
            conn.execute('''
                UPDATE search_sessions 
                SET processed_terms = processed_terms + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (search_session_id,))
            conn.commit()

def update_search_session_progress_batch(search_session_id, increment_count):
    """Update the processed_terms count by a specific increment"""
    with db_lock:
        with get_db_connection() as conn:
            conn.execute('''
                UPDATE search_sessions 
                SET processed_terms = processed_terms + ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (increment_count, search_session_id))
            conn.commit()

def complete_search_session(search_session_id):
    """Mark a search session as completed"""
    with db_lock:
        with get_db_connection() as conn:
            conn.execute('''
                UPDATE search_sessions 
                SET status = 'completed',
                    updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (search_session_id,))
            conn.commit()

def process_term(term, session_id, limit, is_article_search, search_session_id):
    """Process a single search term and return results for batch storage"""
    try:
        # Fetch data from Voila API (unchanged)
        raw_data = fetch_product_data(term, session_id)

        results_to_store = []

        if not raw_data:
            # Return not-found entry
            not_found_result = {
                "found": False,
                "searchTerm": term,
                "productId": None,
                "retailerProductId": None,
                "name": f"Article Not Found: {term}",
                "brand": None,
                "available": False,
                "category": "",
                "imageUrl": None,
                "notFoundMessage": f"The article \"{term}\" was not found. It may not be published yet or could be a typo."
            }
            results_to_store.append((search_session_id, term, json.dumps(not_found_result).decode(), 0))
            return results_to_store

        # Check for product entities (unchanged)
        if "entities" in raw_data and "product" in raw_data["entities"]:
            product_entities = raw_data["entities"]["product"]

            if product_entities:
                total_found = len(product_entities)

                # Different handling for article search vs. generic search (unchanged)
                if is_article_search:
                    # For article search, we typically want the first/best match
                    product_keys = list(product_entities.keys())[:1]
                else:
                    # For generic search, apply the user-specified limit
                    if limit != 'all':
                        try:
                            max_items = int(limit) if isinstance(limit, str) else limit
                            product_keys = list(product_entities.keys())[:max_items]
                        except (ValueError, TypeError):
                            product_keys = list(product_entities.keys())[:10]  # Default to 10
                    else:
                        # For 'all', return all products up to a maximum (50)
                        product_keys = list(product_entities.keys())[:50]

                # Process products and prepare them for batch storage
                if not is_article_search and len(product_keys) > 0:
                    # For generic search, prepare each product for storage
                    for product_id in product_keys:
                        product = product_entities[product_id]
                        product_info = extract_product_info(product, term)
                        results_to_store.append((search_session_id, term, json.dumps(product_info).decode(), total_found))

                    return results_to_store

                # Process just the first product for article searches
                elif product_keys:
                    product_id = product_keys[0]
                    product = product_entities[product_id]

                    # Extract product details
                    try:
                        product_info = extract_product_info(product, term)
                        results_to_store.append((search_session_id, term, json.dumps(product_info).decode(), total_found))
                        return results_to_store
                    except RecursionError:
                        print(f"Recursion error processing product for term {term}")
                        # Return simplified product
                        simplified_result = {
                            "found": True,
                            "searchTerm": term,
                            "productId": product.get("productId"),
                            "name": product.get("name", "Product Name Unavailable"),
                            "brand": product.get("brand", "Brand Unavailable"),
                            "available": False,
                            "category": "",
                            "imageUrl": None,
                            "currentPrice": None,
                            "message": "Product data too complex to fully process"
                        }
                        results_to_store.append((search_session_id, term, json.dumps(simplified_result).decode(), 1))
                        return results_to_store

        # If we get here, no products were found
        not_found_result = {
            "found": False,
            "searchTerm": term,
            "productId": None,
            "retailerProductId": None,
            "name": f"Article Not Found: {term}",
            "brand": None,
            "available": False,
            "category": "",
            "imageUrl": None,
            "notFoundMessage": f"The article \"{term}\" was not found. It may not be published yet or could be a typo."
        }
        results_to_store.append((search_session_id, term, json.dumps(not_found_result).decode(), 0))
        return results_to_store

    except RecursionError:
        print(f"Recursion error processing term {term}")
        error_result = {
            "found": False,
            "searchTerm": term,
            "productId": None,
            "retailerProductId": None,
            "name": f"Processing Error: {term}",
            "brand": None,
            "available": False,
            "category": "",
            "imageUrl": None,
            "notFoundMessage": "Data too complex to process. Try a more specific search term."
        }
        return [(search_session_id, term, json.dumps(error_result).decode(), 0)]
    except Exception as e:
        print(f"Error processing term {term}: {str(e)}")
        # Return error as not found product
        error_result = {
            "found": False,
            "searchTerm": term,
            "productId": None,
            "retailerProductId": None,
            "name": f"Article Not Found: {term}",
            "brand": None,
            "available": False,
            "category": "",
            "imageUrl": None,
            "notFoundMessage": f"Error processing the article. Please try again."
        }
        return [(search_session_id, term, json.dumps(error_result).decode(), 0)]

def process_search_in_background(search_session_id, individual_terms, voila_session_id, limit, is_article_search):
    """Process search terms in background and store results in database"""
    try:
        processed_count = 0

        # Calculate total batches for logging
        total_batches = (len(individual_terms) + BATCH_SIZE - 1) // BATCH_SIZE

        # Reuse a single ThreadPoolExecutor for the whole session
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Process terms in smaller batches to avoid memory issues
            for i in range(0, len(individual_terms), BATCH_SIZE):
                batch_count = i // BATCH_SIZE + 1
                batch_terms = individual_terms[i:i+BATCH_SIZE]
                logging.info(f"Processing batch {batch_count}/{total_batches} with {len(batch_terms)} terms")

                # Collect results for batch storage
                batch_results = []

                # Create futures for batch processing
                futures = {
                    executor.submit(process_term, term, voila_session_id, limit, is_article_search, search_session_id): term 
                    for term in batch_terms
                }

                # Process futures as they complete
                for future in as_completed(futures):
                    term = futures[future]
                    processed_count += 1

                    try:
                        results = future.result()  # This returns a list of results to store
                        if results:
                            batch_results.extend(results)
                    except Exception as e:
                        logging.error(f"Error processing term {term}: {str(e)}")
                        # Add error result to batch
                        error_result = {
                            "found": False,
                            "searchTerm": term,
                            "productId": None,
                            "retailerProductId": None,
                            "name": f"Article Not Found: {term}",
                            "brand": None,
                            "available": False,
                            "category": "",
                            "imageUrl": None,
                            "notFoundMessage": f"The article \"{term}\" was not found. It may not be published yet or could be a typo."
                        }
                        batch_results.append((search_session_id, term, json.dumps(error_result).decode(), 0))

                # Store all results for this batch at once
                if batch_results:
                    store_search_results_batch(batch_results)
                    # Update progress for the entire batch
                    update_search_session_progress_batch(search_session_id, len(batch_terms))

                # Run garbage collection only occasionally for speed
                if GC_ENABLED and batch_count % 3 == 0:  # Every 3rd batch instead of every batch
                    collected = gc.collect()
                    logging.debug(f"Garbage collection: {collected} objects collected")

        # Mark search as completed
        complete_search_session(search_session_id)
        logging.info(f"Search session {search_session_id} completed successfully")

    except Exception as e:
        logging.error(f"Error in background processing for session {search_session_id}: {str(e)}")
        # Mark session as failed
        with db_lock:
            with get_db_connection() as conn:
                conn.execute('''
                    UPDATE search_sessions 
                    SET status = 'failed',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = ?
                ''', (search_session_id,))
                conn.commit()

@app.route('/api/fetch-product', methods=['POST'])
def fetch_product():
    """API endpoint for product searches with user-provided session ID"""
    try:
        data = request.json

        if not data:
            return jsonify({"error": "No request data provided"}), 400

        search_term = data.get('searchTerm')
        session_id = data.get('sessionId')
        limit = data.get('limit', 'all')
        search_type = data.get('searchType', 'article')  # Default to article search

        # Determine if this is an article search or generic search
        is_article_search = search_type == 'article'

        if not search_term:
            return jsonify({"error": "Search term is required"}), 400

        if not session_id:
            return jsonify({"error": "Session ID is required"}), 400

        # Get region info from session ID
        region_info = get_region_info(session_id)

        if not region_info or not region_info.get("regionId"):
            return jsonify({"error": "Could not determine region from session ID"}), 400

        # Parse search terms using the enhanced parser that returns duplicate info
        individual_terms, duplicate_count, contains_ea_codes, duplicates = parse_search_terms(search_term)

        logging.info(f"Processing {len(individual_terms)} individual search terms (removed {duplicate_count} duplicates)")

        # Create a unique search session ID
        search_session_id = str(uuid.uuid4())

        # Store search session in database
        with get_db_connection() as conn:
            conn.execute('''
                INSERT INTO search_sessions 
                (session_id, search_terms, search_type, region_info, total_terms, duplicate_count, duplicates)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                search_session_id,
                search_term,
                search_type,
                json.dumps(region_info).decode(),
                len(individual_terms),
                duplicate_count,
                json.dumps(duplicates).decode() if duplicates else None
            ))
            conn.commit()

        # Start background processing
        threading.Thread(
            target=process_search_in_background,
            args=(search_session_id, individual_terms, session_id, limit, is_article_search),
            daemon=True
        ).start()

        # Return immediate response with search session ID
        response = {
            "search_session_id": search_session_id,
            "region_name": region_info.get("nickname") or "Unknown Region",
            "region_info": region_info,
            "search_term": search_term,
            "parsed_terms": individual_terms,
            "duplicate_count": duplicate_count,
            "duplicates": duplicates,
            "contains_ea_codes": contains_ea_codes,
            "search_type": search_type,
            "total_terms": len(individual_terms),
            "status": "processing"
        }

        return jsonify(response)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/search-status/<search_session_id>', methods=['GET'])
def get_search_status(search_session_id):
    """Get the status of a search session"""
    try:
        with get_db_connection() as conn:
            session = conn.execute('''
                SELECT * FROM search_sessions WHERE session_id = ?
            ''', (search_session_id,)).fetchone()

            if not session:
                return jsonify({"error": "Search session not found"}), 404

            # Get count of processed results
            result_count = conn.execute('''
                SELECT COUNT(*) as count FROM search_results WHERE search_session_id = ?
            ''', (search_session_id,)).fetchone()

            return jsonify({
                "search_session_id": search_session_id,
                "status": session['status'],
                "total_terms": session['total_terms'],
                "processed_terms": session['processed_terms'],
                "result_count": result_count['count'] if result_count else 0,
                "created_at": session['created_at'],
                "updated_at": session['updated_at']
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search-results/<search_session_id>', methods=['GET'])
def get_search_results(search_session_id):
    """Get the results of a search session"""
    try:
        with get_db_connection() as conn:
            # Get session info
            session = conn.execute('''
                SELECT * FROM search_sessions WHERE session_id = ?
            ''', (search_session_id,)).fetchone()

            if not session:
                return jsonify({"error": "Search session not found"}), 404

            # Get all results for this session
            results = conn.execute('''
                SELECT * FROM search_results 
                WHERE search_session_id = ? 
                ORDER BY processed_at ASC
            ''', (search_session_id,)).fetchall()

            # Parse region info and duplicates
            region_info = json.loads(session['region_info']) if session['region_info'] else {}
            duplicates = json.loads(session['duplicates']) if session['duplicates'] else []
            parsed_terms = session['search_terms'].split(',') if session['search_terms'] else []

            # Parse product data from JSON and calculate stats
            products = []
            total_found = 0

            for result in results:
                product_data = json.loads(result['product_data'])
                products.append(product_data)
                if product_data.get('found'):
                    total_found += result['total_found']

            # Count found and not found
            found_products = [p for p in products if p.get('found')]
            not_found_products = [p for p in products if not p.get('found')]

            response = {
                "search_session_id": search_session_id,
                "status": session['status'],
                "region_name": region_info.get("nickname") or "Unknown Region",
                "region_info": region_info,
                "search_term": session['search_terms'],
                "parsed_terms": parsed_terms,
                "duplicate_count": session['duplicate_count'],
                "duplicates": duplicates,
                "search_type": session['search_type'],
                "total_found": total_found,
                "total_processed": len(products),
                "found_count": len(found_products),
                "not_found_count": len(not_found_products),
                "products": products
            }

            return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
