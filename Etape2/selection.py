import pandas as pd
import mysql.connector
import numpy as np
import re

# --- Database Configurations ---
DB_CONFIG_SHOPIFY = {
    'host': 'localhost', 'user': 'root', 'password': '', 'database': 'shopify_data'
}
DB_CONFIG_WOOCOMMERCE = {
    'host': 'localhost', 'user': 'root', 'password': '', 'database': 'web_scraping_db'
}
DB_CONFIG_ANALYSIS = {
    'host': 'localhost', 'user': 'root', 'password': '', 'database': 'product_analysis_db'
}

# --- Analysis Configuration ---
TOP_K_OVERALL = 20
FLAGSHIP_PER_STORE = 3
WEIGHT_AVAILABILITY = 0.6
WEIGHT_PRICE = 0.4
DB_BATCH_SIZE = 500 # For saving scored_products

# --- DB Connection Function (reusable) ---
def db_connect(config, attempt_creation=False):
    db_name = config['database']
    temp_config = config.copy()
    conn_server = None # Initialize for finally block
    cursor_server = None # Initialize for finally block
    if attempt_creation:
        if 'database' in temp_config:
            del temp_config['database']
        try:
            conn_server = mysql.connector.connect(**temp_config)
            cursor_server = conn_server.cursor()
            cursor_server.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            print(f"Database '{db_name}' checked/created.")
        except mysql.connector.Error as err:
            print(f"Error during database creation check for '{db_name}': {err}")
        finally:
            if cursor_server: cursor_server.close()
            if conn_server and conn_server.is_connected(): conn_server.close()
    
    try:
        conn = mysql.connector.connect(**config)
        print(f"Successfully connected to MySQL database: {db_name}")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL ({db_name}): {err}")
        return None

# --- Fetch Functions ---
def fetch_shopify_data(conn):
    df = pd.DataFrame()
    if not conn: return df
    try:
        query = "SELECT product_url, title, vendor, price, availability, description, category AS product_category, store_name AS source_store_name FROM products WHERE price IS NOT NULL AND title IS NOT NULL"
        df = pd.read_sql(query, conn)
        if not df.empty:
            df['source_platform'] = 'Shopify'
            df['product_tags'] = None 
            df['sku'] = None
        print(f"Fetched {len(df)} products from Shopify.")
    except Exception as e: print(f"Error fetching Shopify data: {e}")
    return df

def fetch_woocommerce_data(conn):
    df = pd.DataFrame()
    if not conn: return df
    try:
        query = "SELECT product_url, title, price, tag AS product_tags, sku, category AS product_category FROM barefoot_products WHERE price IS NOT NULL AND title IS NOT NULL"
        df = pd.read_sql(query, conn)
        if not df.empty:
            df['source_platform'] = 'WooCommerce'
            df['source_store_name'] = 'Barefoot Buttons'
            df['vendor'] = 'Barefoot Buttons'
            df['availability'] = 'Available' 
            df['description'] = None
        print(f"Fetched {len(df)} products from WooCommerce (Barefoot Buttons).")
    except Exception as e: print(f"Error fetching WooCommerce data: {e}")
    return df

# --- Preprocessing and Scoring Functions ---
def clean_html(raw_html):
    if pd.isna(raw_html) or not isinstance(raw_html, str): return ''
    return re.sub(re.compile('<.*?>'), '', raw_html).strip()

def preprocess_combined_data(df):
    if df.empty: return df
    print("\n--- Preprocessing Combined Data ---")
    if 'price' in df.columns:
        def clean_and_convert_price(price_val):
            if pd.isna(price_val): return np.nan
            if isinstance(price_val, (int, float)): return float(price_val)
            try:
                cleaned_price = re.sub(r'[^\d\.]', '', str(price_val))
                return float(cleaned_price) if cleaned_price else np.nan
            except ValueError: return np.nan
        df.loc[:, 'price'] = df['price'].apply(clean_and_convert_price)
        df.dropna(subset=['price'], inplace=True)
    if 'availability' in df.columns:
        df.loc[:, 'is_available_numeric'] = df['availability'].apply(lambda x: 1 if isinstance(x, str) and x.lower() == 'available' else 0)
    else: df['is_available_numeric'] = 0
    if 'description' in df.columns:
        df.loc[:, 'description_text'] = df['description'].apply(clean_html)
    else: df['description_text'] = ''
    
    for col in ['title', 'vendor', 'product_category', 'source_store_name', 'product_tags', 'sku']:
        if col in df.columns:
            df.loc[:, col] = df[col].fillna('N/A')
        else: 
            df[col] = 'N/A'
            
    print(f"Combined data preprocessing complete. DataFrame shape: {df.shape}")
    return df

def calculate_attractiveness_score(df, w_availability, w_price):
    if df.empty or not all(col in df.columns for col in ['is_available_numeric', 'price']):
        df['final_score'] = 0.0
        print("Skipping scoring: empty DataFrame or missing 'is_available_numeric'/'price'.")
        return df
    print("\n--- Calculating Attractiveness Score ---")
    df['availability_score'] = df['is_available_numeric']
    
    price_score_col = pd.Series(0.0, index=df.index, name='price_score')
    if df['price'].nunique() > 1 and not df['price'].isnull().all():
        price_min = df['price'].min()
        price_max = df['price'].max()
        if price_max == price_min: price_score_col.loc[:] = 0.5
        else: price_score_col.loc[:] = (price_max - df['price']) / (price_max - price_min)
    elif len(df['price']) > 0 and not df['price'].isnull().all(): price_score_col.loc[:] = 0.5
    
    df['price_score'] = price_score_col.fillna(0)
    df['final_score'] = (df['availability_score'] * w_availability) + (df['price_score'] * w_price)
    print("Attractiveness scores calculated.")
    return df

# --- Functions to Save Analysis Results to DB ---
def create_analysis_tables(conn_analysis):
    if not conn_analysis: return
    cursor = None
    try:
        cursor = conn_analysis.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scored_products (
                id INT AUTO_INCREMENT PRIMARY KEY, product_url VARCHAR(1024) NOT NULL, title VARCHAR(512),
                source_store_name VARCHAR(100), price DECIMAL(10, 2), is_available_numeric TINYINT,
                description_text TEXT, product_category VARCHAR(255), product_tags TEXT, sku VARCHAR(100),
                source_platform VARCHAR(50), availability_score FLOAT, price_score FLOAT, final_score FLOAT,
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uni_scored_product_url (product_url(255)),
                INDEX idx_final_score (final_score DESC), INDEX idx_store_platform (source_store_name, source_platform)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_k_products_overall (
                rank_overall INT, product_url VARCHAR(1024) NOT NULL, title VARCHAR(512),
                source_store_name VARCHAR(100), final_score FLOAT, source_platform VARCHAR(50),
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (product_url(255))
            ) ENGINE= InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flagship_products_by_store (
                id INT AUTO_INCREMENT PRIMARY KEY, source_store_name VARCHAR(100), store_rank INT,
                product_url VARCHAR(1024) NOT NULL, title VARCHAR(512), final_score FLOAT, source_platform VARCHAR(50),
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_store_product (source_store_name, product_url(255)),
                INDEX idx_store_name_flagship (source_store_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS store_rankings (
                id INT AUTO_INCREMENT PRIMARY KEY, source_store_name VARCHAR(100) UNIQUE,
                avg_product_score FLOAT, max_product_score FLOAT, source_platform VARCHAR(50),
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
        conn_analysis.commit()
        print("Analysis tables checked/created successfully in product_analysis_db.")
    except mysql.connector.Error as err:
        print(f"Error creating analysis tables: {err}")
    finally:
        if cursor: cursor.close()

def save_scored_products_to_db(df, conn_analysis, batch_size=DB_BATCH_SIZE):
    if df.empty or not conn_analysis: return
    print(f"\n--- Saving Scored Products to Database (batch size: {batch_size}) ---")
    
    cols_for_db = [
        'product_url', 'title', 'source_store_name', 'price', 'is_available_numeric',
        'description_text', 'product_category', 'product_tags', 'sku', 'source_platform',
        'availability_score', 'price_score', 'final_score'
    ]
    for col in cols_for_db:
        if col not in df.columns:
            df[col] = None 
    df_to_save = df[cols_for_db].copy()

    numeric_cols_to_nullify_nans = ['price', 'availability_score', 'price_score', 'final_score']
    for col in numeric_cols_to_nullify_nans:
        if col in df_to_save.columns:
            df_to_save.loc[:, col] = df_to_save[col].astype(object).where(pd.notnull(df_to_save[col]), None)
    
    text_cols_to_na = ['product_url', 'title', 'source_store_name', 'description_text', 
                       'product_category', 'product_tags', 'sku', 'source_platform']
    for col in text_cols_to_na:
        if col in df_to_save.columns:
             df_to_save.loc[:, col] = df_to_save[col].fillna('N/A').astype(str)
        else: 
             df_to_save[col] = 'N/A'

    sql = """
    INSERT INTO scored_products (product_url, title, source_store_name, price, is_available_numeric,
                                 description_text, product_category, product_tags, sku, source_platform,
                                 availability_score, price_score, final_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title), source_store_name=VALUES(source_store_name), price=VALUES(price),
        is_available_numeric=VALUES(is_available_numeric), description_text=VALUES(description_text),
        product_category=VALUES(product_category), product_tags=VALUES(product_tags), sku=VALUES(sku),
        source_platform=VALUES(source_platform), availability_score=VALUES(availability_score),
        price_score=VALUES(price_score), final_score=VALUES(final_score),
        analysis_timestamp=CURRENT_TIMESTAMP;
    """
    
    all_data_tuples = [tuple(x) for x in df_to_save.to_numpy()]
    total_saved_count = 0
    
    for i in range(0, len(all_data_tuples), batch_size):
        batch_tuples = all_data_tuples[i:i + batch_size]
        cursor = None
        try:
            if not conn_analysis.is_connected():
                print("Reconnecting to analysis DB for batch...")
                conn_analysis.reconnect(attempts=3, delay=5)
                if not conn_analysis.is_connected():
                    print("Failed to reconnect. Skipping this batch.")
                    continue
            cursor = conn_analysis.cursor()
            cursor.executemany(sql, batch_tuples)
            conn_analysis.commit()
            total_saved_count += len(batch_tuples)
            print(f"Saved/Updated batch of {len(batch_tuples)} products. Total so far: {total_saved_count}")
        except mysql.connector.Error as err:
            print(f"DB Error saving batch of scored products: {err}")
            if conn_analysis.is_connected(): conn_analysis.rollback()
        except Exception as e:
            print(f"General error saving batch of scored products: {e}")
        finally:
            if cursor: cursor.close()
    
    print(f"Finished saving/updating a total of {total_saved_count} products in 'scored_products' table.")

def save_top_k_to_db(top_k_df, conn_analysis):
    if top_k_df.empty or not conn_analysis: return
    print(f"\n--- Saving Top {len(top_k_df)} Products to Database ---")
    cursor = None
    try:
        cursor = conn_analysis.cursor()
        cursor.execute("DELETE FROM top_k_products_overall;") 
        sql = "INSERT INTO top_k_products_overall (rank_overall, product_url, title, source_store_name, final_score, source_platform) VALUES (%s, %s, %s, %s, %s, %s)"
        data_tuples = [(i + 1, row.get('product_url', 'N/A'), row.get('title', 'N/A'), row.get('source_store_name', 'N/A'), row.get('final_score'), row.get('source_platform', 'N/A'))
                       for i, row in top_k_df.reset_index(drop=True).iterrows()]
        cursor.executemany(sql, data_tuples)
        conn_analysis.commit()
        print(f"Saved {len(data_tuples)} products in 'top_k_products_overall' table.")
    except mysql.connector.Error as err: print(f"DB Error saving top_k products: {err}")
    finally: 
        if cursor: cursor.close()

def save_flagship_to_db(flagship_df, conn_analysis):
    if flagship_df.empty or not conn_analysis: return
    print(f"\n--- Saving Flagship Products to Database ---")
    cursor = None
    try:
        cursor = conn_analysis.cursor()
        cursor.execute("DELETE FROM flagship_products_by_store;")
        sql = "INSERT INTO flagship_products_by_store (source_store_name, store_rank, product_url, title, final_score, source_platform) VALUES (%s, %s, %s, %s, %s, %s)"
        data_tuples = []
        
        ranked_flagship_list = []
        if 'source_store_name' in flagship_df.columns:
            for _, group in flagship_df.groupby('source_store_name'):
                group_copy = group.copy() # Explicitly work on a copy
                group_copy.loc[:, 'store_rank'] = range(1, len(group_copy) + 1)
                ranked_flagship_list.append(group_copy)
        
        if not ranked_flagship_list:
            print("No flagship products after ranking to save.")
            return 
            
        ranked_flagship_df = pd.concat(ranked_flagship_list)

        for _, row in ranked_flagship_df.iterrows():
             data_tuples.append((
                row.get('source_store_name', 'N/A'), row.get('store_rank', 0), row.get('product_url', 'N/A'),
                row.get('title', 'N/A'), row.get('final_score'), row.get('source_platform', 'N/A')
            ))
        cursor.executemany(sql, data_tuples)
        conn_analysis.commit()
        print(f"Saved {len(data_tuples)} products in 'flagship_products_by_store' table.")
    except mysql.connector.Error as err: print(f"DB Error saving flagship products: {err}")
    finally: 
        if cursor: cursor.close()

def save_store_rankings_to_db(avg_scores_series, max_scores_series, platform_map_df, conn_analysis):
    if (avg_scores_series.empty and max_scores_series.empty) or not conn_analysis: return
    print("\n--- Saving Store Rankings to Database ---")
    cursor = None
    
    rankings_df = pd.DataFrame({'avg_product_score': avg_scores_series, 'max_product_score': max_scores_series}).reset_index() 
    if not platform_map_df.empty:
        rankings_df = pd.merge(rankings_df, platform_map_df, on='source_store_name', how='left')
    else:
        rankings_df['source_platform'] = 'Unknown'
    
    # Ensure 'source_platform' column exists after merge before fillna
    if 'source_platform' not in rankings_df.columns:
        rankings_df['source_platform'] = 'Unknown'
    else:
        rankings_df.loc[:, 'source_platform'] = rankings_df['source_platform'].fillna('Unknown')
    
    try:
        cursor = conn_analysis.cursor()
        cursor.execute("DELETE FROM store_rankings;")
        sql = "INSERT INTO store_rankings (source_store_name, avg_product_score, max_product_score, source_platform) VALUES (%s, %s, %s, %s)"
        data_tuples = [(row['source_store_name'], row.get('avg_product_score'), row.get('max_product_score'), row['source_platform'])
                       for _, row in rankings_df.iterrows()]
        cursor.executemany(sql, data_tuples)
        conn_analysis.commit()
        print(f"Saved {len(data_tuples)} entries in 'store_rankings' table.")
    except mysql.connector.Error as err: print(f"DB Error saving store rankings: {err}")
    finally: 
        if cursor: cursor.close()

# --- Display functions ---
def display_top_k_products(df, k):
    if df.empty or 'final_score' not in df.columns: return pd.DataFrame()
    print(f"\n--- Top {k} Most Attractive Products (Overall - Combined) ---")
    top_k = df.sort_values(by='final_score', ascending=False).head(k)
    print(top_k[['title', 'source_store_name', 'price', 'is_available_numeric', 'final_score', 'product_url', 'source_platform']])
    return top_k

def display_flagship_products_per_store(df, n_flagship):
    if df.empty or not all(col in df.columns for col in ['source_store_name', 'final_score']): return pd.DataFrame()
    print(f"\n--- Top {n_flagship} Flagship Products per Store (Combined) ---")
    flagship = df.loc[df.groupby('source_store_name')['final_score'].nlargest(n_flagship).index.get_level_values(1)]
    if flagship.empty: print("No flagship products to display.")
    else: print(flagship[['source_store_name', 'title', 'final_score', 'price', 'product_url', 'source_platform']])
    return flagship

def display_store_rankings(df):
    if df.empty or not all(col in df.columns for col in ['source_store_name', 'final_score']): return pd.Series(dtype=float), pd.Series(dtype=float)
    print("\n--- Store Rankings (Combined) ---")
    avg_store_scores = df.groupby('source_store_name')['final_score'].mean().sort_values(ascending=False)
    print("\nRanked by Average Product Score:"); print(avg_store_scores)
    max_store_scores = df.groupby('source_store_name')['final_score'].max().sort_values(ascending=False)
    print("\nRanked by Best Product Score:"); print(max_store_scores)
    return avg_store_scores, max_store_scores

# --- Main Execution ---
if __name__ == "__main__":
    conn_shopify = db_connect(DB_CONFIG_SHOPIFY)
    conn_woocommerce = db_connect(DB_CONFIG_WOOCOMMERCE)
    conn_analysis = db_connect(DB_CONFIG_ANALYSIS, attempt_creation=True)

    if conn_analysis: create_analysis_tables(conn_analysis)

    df_shopify_raw = fetch_shopify_data(conn_shopify)
    df_woocommerce_raw = fetch_woocommerce_data(conn_woocommerce)

    if conn_shopify and conn_shopify.is_connected(): conn_shopify.close(); print(f"MySQL connection to {DB_CONFIG_SHOPIFY['database']} closed.")
    if conn_woocommerce and conn_woocommerce.is_connected(): conn_woocommerce.close(); print(f"MySQL connection to {DB_CONFIG_WOOCOMMERCE['database']} closed.")

    expected_cols = ['product_url', 'title', 'vendor', 'price', 'availability',
                     'description', 'product_category', 'source_store_name',
                     'source_platform', 'product_tags', 'sku']
    
    df_s_list = []
    if not df_shopify_raw.empty:
        current_cols = df_shopify_raw.columns.tolist()
        for col in expected_cols:
            if col not in current_cols: df_shopify_raw[col] = None
        df_s_list.append(df_shopify_raw[expected_cols])
    
    if not df_woocommerce_raw.empty:
        current_cols = df_woocommerce_raw.columns.tolist()
        for col in expected_cols:
            if col not in current_cols: df_woocommerce_raw[col] = None
        df_s_list.append(df_woocommerce_raw[expected_cols])

    combined_df = pd.DataFrame(columns=expected_cols) 

    if df_s_list:
        combined_df = pd.concat(df_s_list, ignore_index=True)
        combined_df.dropna(how='all', inplace=True)
        if 'product_url' in combined_df.columns:
            combined_df.drop_duplicates(subset=['product_url'], keep='first', inplace=True)
        print(f"\nCombined DataFrame created. Shape after dropna/duplicates: {combined_df.shape}")

        if not combined_df.empty:
            combined_df = preprocess_combined_data(combined_df)
            if not combined_df.empty:
                combined_df = calculate_attractiveness_score(combined_df, WEIGHT_AVAILABILITY, WEIGHT_PRICE)
                
                top_k_df = display_top_k_products(combined_df, TOP_K_OVERALL)
                flagship_df_display = display_flagship_products_per_store(combined_df, FLAGSHIP_PER_STORE)
                avg_scores, max_scores = display_store_rankings(combined_df)

                if conn_analysis:
                    save_scored_products_to_db(combined_df, conn_analysis)
                    save_top_k_to_db(top_k_df, conn_analysis)
                    save_flagship_to_db(flagship_df_display, conn_analysis)
                    
                    platform_map_df = pd.DataFrame()
                    if not combined_df.empty and 'source_store_name' in combined_df.columns and 'source_platform' in combined_df.columns:
                        platform_map_df = combined_df.drop_duplicates(subset=['source_store_name'])[['source_store_name', 'source_platform']].set_index('source_store_name')
                    
                    save_store_rankings_to_db(avg_scores, max_scores, platform_map_df, conn_analysis)
            else: print("Analysis halted: Combined DataFrame empty after preprocessing.")
        else: print("Analysis halted: Combined DataFrame empty after initial combination/deduplication.")
    else: print("Analysis halted: No data fetched from any database.")

    if conn_analysis and conn_analysis.is_connected():
        conn_analysis.close()
        print(f"MySQL connection to {DB_CONFIG_ANALYSIS['database']} closed.")
    print("\n--- Combined Analysis Complete ---")