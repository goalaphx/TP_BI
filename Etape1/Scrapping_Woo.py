from requests_html import HTMLSession
import time
import mysql.connector
from urllib.parse import urljoin # To correctly join relative URLs

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root', # Replace with your MySQL username
    'password': '', # Replace with your MySQL password
    'database': 'web_scraping_db' # The database for WooCommerce/Barefoot data
}

# --- Configuration for Barefoot Buttons Categories ---
BAREFOOT_CATEGORIES_TO_SCRAPE = [
    {
        'name': 'Version 1', # This name will be stored as the 'category' in the DB
        'url': 'https://barefootbuttons.com/product-category/version-1/',
    },
    {
        'name': 'Version 2',
        'url': 'https://barefootbuttons.com/product-category/version-2/',
    },
    {
        'name': 'Mini',
        'url': 'https://barefootbuttons.com/product-category/mini/',
    },
    {
        'name': 'Tallboy',
        'url': 'https://barefootbuttons.com/product-category/tallboy/',
    },
    # Add more categories from barefootbuttons.com as needed
    # Example:
    # {
    #     'name': 'Caps',
    #     'url': 'https://barefootbuttons.com/product-category/caps/',
    # },
]

# --- Global Session and Headers ---
s = HTMLSession()
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
}
s.headers.update(HEADERS)

def db_connect():
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print(f"Successfully connected to MySQL database: {DB_CONFIG['database']}")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL ({DB_CONFIG['database']}): {err}")
        return None

def create_barefoot_table_if_not_exists(cursor):
    """Creates the barefoot_products table if it doesn't already exist."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS barefoot_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_url VARCHAR(1024) UNIQUE,
                title VARCHAR(255) NOT NULL,
                price VARCHAR(50),
                tag VARCHAR(255), -- This was 'tag' from your original simple script
                sku VARCHAR(100),
                category VARCHAR(100), -- NEW: To store the scraped category name
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
        # Ensure category column exists if table was already there
        try:
            cursor.execute("ALTER TABLE barefoot_products ADD COLUMN category VARCHAR(100) AFTER sku;")
            print("Ensured 'category' column exists in 'barefoot_products'.")
        except mysql.connector.Error as alter_err:
            if alter_err.errno == 1060: # Duplicate column name
                pass # Column already exists, which is fine
            else:
                raise # Re-raise other alter errors
        print("Table 'barefoot_products' checked/created successfully.")
    except mysql.connector.Error as err:
        print(f"Error with barefoot_products table setup: {err}")


def insert_product_data(db_conn, product_data, product_url, category_name_from_config):
    """Inserts or updates product data into the barefoot_products table."""
    if not db_conn:
        print("No database connection. Skipping insert.")
        return

    cursor = db_conn.cursor()
    sql = """
    INSERT INTO barefoot_products (product_url, title, price, tag, sku, category)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        price = VALUES(price),
        tag = VALUES(tag),
        sku = VALUES(sku),
        category = VALUES(category),
        scraped_at = CURRENT_TIMESTAMP;
    """
    try:
        values = (
            product_url,
            product_data.get('title', 'N/A'),
            product_data.get('price', 'N/A'),
            product_data.get('tag', 'N/A'), # This was the 'tag' from your original script
            product_data.get('sku', 'N/A'),
            category_name_from_config # Use the category name from our config
        )
        cursor.execute(sql, values)
        # print(f"Data for '{product_data.get('title', 'Unknown')}' queued for DB.") # Less verbose
    except mysql.connector.Error as err:
        print(f"DB Error for {product_url}: {err}")
    finally:
        cursor.close()

def fetch_page_with_retries(url, retries=3, delay=5, timeout=25):
    for i in range(retries):
        try:
            r = s.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"Error fetching {url} (Attempt {i+1}/{retries}): {e}")
            if i < retries - 1: time.sleep(delay)
            else: return None

def get_product_links_from_category_page(page_url):
    print(f"Fetching product links from: {page_url}")
    r = fetch_page_with_retries(page_url)
    if not r or not r.html:
        print(f"Failed to fetch/parse HTML for {page_url}")
        return [], None

    product_item_selector = 'div.product-small.box' # Barefoot Buttons specific
    items = r.html.find(product_item_selector)
    links = []
    if not items: print(f"No product items found on {page_url} with selector '{product_item_selector}'.")

    for item in items:
        # Try a more specific link first, then fallback
        link_tag = item.find('a.woocommerce-LoopProduct-link', first=True)
        if not link_tag:
            link_tag = item.find('p.name.product-title > a', first=True) # Link around title text
        if not link_tag:
            link_tag = item.find('a', first=True) # General first link

        if link_tag and 'href' in link_tag.attrs:
            links.append(urljoin(page_url, link_tag.attrs['href']))
        else:
            print(f"Warning: Product item on {page_url} missing valid link. Item HTML: {item.html[:100]}...")

    next_page_selector = 'a.next.page-numbers' # Common WooCommerce pagination
    next_page_tag = r.html.find(next_page_selector, first=True)
    next_page_url = None
    if next_page_tag and 'href' in next_page_tag.attrs:
        next_page_url = urljoin(page_url, next_page_tag.attrs['href'])
        print(f"Found next page: {next_page_url}")
    else:
        print(f"No 'Next Page' link found on {page_url} (selector: '{next_page_selector}'). End of category or JS pagination.")
    return links, next_page_url

def get_all_product_links_for_category(start_category_url):
    all_links_for_category = []
    current_page_url = start_category_url
    max_pages = 20 # Safety limit
    pages_scraped = 0

    while current_page_url and pages_scraped < max_pages:
        pages_scraped += 1
        print(f"\n--- Scraping links from page {pages_scraped} of category: {current_page_url} ---")
        links_on_page, next_page_url_candidate = get_product_links_from_category_page(current_page_url)
        
        newly_added = 0
        if links_on_page:
            for link in links_on_page:
                if link not in all_links_for_category:
                    all_links_for_category.append(link)
                    newly_added +=1
            print(f"Collected {newly_added} new links. Total unique links for this category: {len(all_links_for_category)}")
        else:
            print(f"No links found on {current_page_url}.")

        if next_page_url_candidate and next_page_url_candidate != current_page_url:
            current_page_url = next_page_url_candidate
            time.sleep(1.5)
        else:
            if next_page_url_candidate == current_page_url and next_page_url_candidate is not None :
                 print(f"Warning: Next page URL is same as current. Stopping pagination.")
            current_page_url = None
            
    if pages_scraped == max_pages and current_page_url:
        print(f"Warning: Reached max_pages ({max_pages}) for {start_category_url}.")
    return all_links_for_category

def get_product_data(product_url):
    print(f"Scraping product data from: {product_url}")
    r = fetch_page_with_retries(product_url)
    if not r or not r.html: return None

    product_details = {}
    try:
        title_el = r.html.find('h1.product_title.entry-title', first=True)
        product_details['title'] = title_el.full_text.strip() if title_el else 'N/A'

        price_elements = r.html.find('span.woocommerce-Price-amount.amount bdi')
        if len(price_elements) > 1:
            product_details['price'] = price_elements[1].full_text.strip()
        elif price_elements:
            product_details['price'] = price_elements[0].full_text.strip()
        else:
            price_any = r.html.find('p.price span.woocommerce-Price-amount.amount', first=True) # Broader price
            product_details['price'] = price_any.text.strip() if price_any else 'N/A'

        tag_el = r.html.find('span.tagged_as a[rel=tag]', first=True) # The product 'tag'
        product_details['tag'] = tag_el.full_text.strip() if tag_el else 'N/A'
        
        sku_el = r.html.find('span.sku', first=True)
        product_details['sku'] = sku_el.full_text.strip() if sku_el else 'N/A'
        
        # Note: 'category' field is populated from BAREFOOT_CATEGORIES_TO_SCRAPE.name, not scraped here.

        print(f"Scraped: {product_details}")
        return product_details
    except Exception as e:
        print(f"Error parsing product data for {product_url}: {e}")
        return {k: 'N/A (Parse Error)' for k in ['title', 'price', 'tag', 'sku']}


# --- Main Script Logic ---
def main():
    db_connection = db_connect()
    if not db_connection:
        print("Could not connect to database. Exiting.")
        return
    
    cursor = db_connection.cursor()
    create_barefoot_table_if_not_exists(cursor) # Ensure table and category column exist
    cursor.close()

    total_products_processed_for_db = 0

    for category_config in BAREFOOT_CATEGORIES_TO_SCRAPE:
        category_name_for_db = category_config['name'] # This will be stored as 'category'
        category_start_url = category_config['url']
        print(f"\n{'='*20} Processing Category: {category_name_for_db} ({category_start_url}) {'='*20}")

        product_page_links = get_all_product_links_for_category(category_start_url)

        if not product_page_links:
            print(f"No product links found for category '{category_name_for_db}'. Skipping.")
            continue

        print(f"\nFound {len(product_page_links)} total unique product links for '{category_name_for_db}'. Extracting data...")
        
        products_in_this_category_db = 0
        for i, link in enumerate(product_page_links):
            print(f"Processing product {i+1}/{len(product_page_links)} for '{category_name_for_db}'...")
            product_info = get_product_data(link)
            if product_info:
                insert_product_data(db_connection, product_info, link, category_name_for_db)
                products_in_this_category_db +=1
            time.sleep(1) # Be respectful between product page scrapes

        db_connection.commit() # Commit after each category is fully processed
        print(f"Category '{category_name_for_db}' completed. {products_in_this_category_db} products processed for DB.")
        total_products_processed_for_db += products_in_this_category_db
        time.sleep(3) # Pause between categories

    db_connection.close()
    print(f"\nDone scraping all Barefoot Buttons categories. Total products processed for DB: {total_products_processed_for_db}")

if __name__ == '__main__':
    main()