import requests
import mysql.connector
import time

# List of Shopify stores
stores = [
    "https://www.allbirds.com",
    "https://www.brooklinen.com",
    "https://www.untuckit.com",
    "https://tattly.com",
    "https://flowrette.com"
    # Add more stores as needed
]

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',         # Replace with your MySQL username
    'password': '',         # Replace with your MySQL password (leave empty if root has no password)
    'database': 'shopify_data'
}

# --- HTTP Headers ---
# It's good practice to set a User-Agent. You can customize this.
REQUEST_HEADERS = {
    'User-Agent': 'MyProductScraper/1.0 (contact:youremail@example.com; purpose:data collection for project XYZ)'
    # Or a common browser User-Agent:
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
}

def db_connect():
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("Successfully connected to MySQL database.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def create_table_if_not_exists(cursor):
    """Creates the products table if it doesn't already exist."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_url VARCHAR(1024) UNIQUE,
                title VARCHAR(512) NOT NULL,
                vendor VARCHAR(255),
                price DECIMAL(10, 2),
                availability VARCHAR(50),
                description TEXT,
                category VARCHAR(255),
                store_name VARCHAR(100),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
        print("Table 'products' checked/created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")


# --- Main Script Logic ---
def main():
    db_connection = db_connect()
    if not db_connection:
        print("Could not connect to database. Exiting.")
        return

    cursor = db_connection.cursor()
    create_table_if_not_exists(cursor) # Ensure table exists

    total_products_affected = 0

    for base_url in stores:
        # Simple store name extraction (can be improved if needed)
        store_name_parts = base_url.replace("https://www.", "").replace("https://", "").split('.')
        store_name = store_name_parts[0] if store_name_parts else base_url

        print(f"\nScraping store: {store_name} from {base_url}")
        page = 1
        products_this_store_count = 0

        while True:
            # Use limit=250 for fewer requests
            url = f"{base_url}/products.json?page={page}&limit=250"
            print(f"Fetching: {url}")

            try:
                response = requests.get(url, headers=REQUEST_HEADERS, timeout=30) # Increased timeout
                response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
            except requests.exceptions.HTTPError as http_err:
                status_code = http_err.response.status_code
                if status_code == 404:
                    print(f"Page {page} not found for {store_name} (404), likely end of products for this store.")
                elif status_code == 401 or status_code == 403:
                    print(f"Access Denied (401/403) for {url}. Store might be private or block scraping.")
                elif status_code == 429: # Too Many Requests
                    print(f"Rate limited (429) at {url}. Waiting 60 seconds before trying next store or stopping.")
                    time.sleep(60) # Simple wait, could implement exponential backoff
                else:
                    print(f"HTTP error fetching {url}: {http_err}")
                break # Stop processing this store on HTTP errors
            except requests.exceptions.RequestException as req_err: # Other errors (timeout, connection)
                print(f"Request error fetching {url}: {req_err}")
                break # Stop processing this store

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                print(f"Failed to decode JSON from {url}. Content snippet: {response.text[:200]}")
                break # Stop processing this store

            products_on_page = data.get("products", [])
            if not products_on_page:
                if page == 1:
                    print(f"No products found on the first page for {store_name}. The /products.json endpoint might be disabled or empty.")
                else:
                    print(f"No more products found on page {page} for {store_name}.")
                break # End of products for this store

            for product in products_on_page:
                try:
                    title = product.get('title', 'N/A')
                    vendor = product.get('vendor', 'N/A')

                    # Safely get first variant's data
                    variants = product.get('variants', [])
                    first_variant = variants[0] if variants else {} # Default to empty dict if no variants

                    price_str = first_variant.get('price', '0.0')
                    price = float(price_str) if price_str else 0.0

                    availability = "Available" if first_variant.get('available', False) else "Out of Stock"
                    
                    description = product.get('body_html', '') # Often contains HTML tags
                    category = product.get('product_type', 'N/A')
                    handle = product.get('handle')
                    product_link = f"{base_url}/products/{handle}" if handle else 'N/A'


                    # SQL query to insert or update data
                    # Ensure product_url column has a UNIQUE constraint in your DB for this to work
                    sql_query = """
                    INSERT INTO products (product_url, title, vendor, price, availability, description, category, store_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        vendor = VALUES(vendor),
                        price = VALUES(price),
                        availability = VALUES(availability),
                        description = VALUES(description),
                        category = VALUES(category),
                        store_name = VALUES(store_name),
                        scraped_at = CURRENT_TIMESTAMP;
                    """
                    values = (
                        product_link, title, vendor, price, availability,
                        description, category, store_name
                    )
                    cursor.execute(sql_query, values)
                    total_products_affected += 1
                    products_this_store_count +=1

                except KeyError as ke:
                    print(f"Skipping product (KeyError: {ke}) in '{title if 'title' in locals() else 'Unknown Title'}'. Data: {str(product)[:100]}...")
                except ValueError as ve:
                    print(f"Skipping product (ValueError: {ve}) in '{title if 'title' in locals() else 'Unknown Title'}', likely price conversion. Price string: '{price_str if 'price_str' in locals() else 'Unknown'}'.")
                except Exception as e:
                    print(f"Skipping product '{title if 'title' in locals() else 'Unknown Title'}' due to an unexpected error: {e}")

            db_connection.commit() # Commit after processing all products on a page
            print(f"Page {page} for {store_name} (found {len(products_on_page)} products) committed to DB. Total for this store so far: {products_this_store_count}")
            page += 1
            time.sleep(1.5) # Be respectful, slight increase

        print(f"Finished scraping {store_name}. Total products from this store: {products_this_store_count}")
        time.sleep(3) # Pause between different stores

    cursor.close()
    db_connection.close()
    print(f"\nDone scraping all stores. Total products affected (inserted/updated): {total_products_affected}")

if __name__ == '__main__':
    main()