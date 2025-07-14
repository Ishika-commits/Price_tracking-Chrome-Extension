import scrapy
import time
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from random import randint


class AmazonSeleniumScrapy:
    def __init__(self, pincode, search_query):
        self.pincode = pincode
        self.search_query = search_query
        self.cookies = []
        self.driver = None
        self.product_urls = []

    def setup_chrome_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.64 Safari/537.36")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--enable-automation")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--incognito")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(100)

    def setup_pincode_and_get_cookies(self):
        try:
            self.driver.get("https://www.amazon.in")
            time.sleep(3)

            location_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "nav-global-location-slot"))
            )
            location_element.click()
            time.sleep(randint(2, 4))

            pincode_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput"))
            )
            pincode_input.clear()
            time.sleep(1)
            pincode_input.send_keys(self.pincode)
            time.sleep(1)

            update_button = self.driver.find_element(By.ID, "GLUXZipUpdate")
            update_button.click()
            time.sleep(6)

            self.cookies = self.driver.get_cookies()
            print(f"Pincode {self.pincode} is set and cookies collected.")
            return True

        except Exception as e:
            print(f"Failed to apply pincode: {e}")
            return False

    def search_and_get_product_urls(self):
        try:
            search_url = f"https://www.amazon.in/s?k={self.search_query.replace(' ', '+')}"
            self.driver.get(search_url)
            time.sleep(5)

            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-component-type="s-search-result"]'))
            )

            product_selectors = [
                'a.a-link-normal.s-no-outline',
                'h2.a-size-mini.a-spacing-none.a-color-base.s-size-base-plus a',
                '[data-component-type="s-search-result"] h2 a',
                '.s-result-item h2 a',
                'a[href*="/dp/"]'
            ]
            
            product_elements = []
            for selector in product_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    product_elements = elements
                    print(f"Found {len(elements)} elements with selector: {selector}")
                    break
            
            if not product_elements:
                print("No product elements found with any selector")
                return []

            seen_urls = set()
            self.product_urls = []
            
            for element in product_elements:
                try:
                    href = element.get_attribute('href')
                    if href and '/dp/' in href and href not in seen_urls:
                        clean_url = href.split('?')[0] if '?' in href else href
                        if '/dp/' in clean_url:
                            self.product_urls.append(clean_url)
                            seen_urls.add(href)
                            
                            if len(self.product_urls) >= 10:
                                break
                except Exception as e:
                    print(f"Error processing element: {e}")
                    continue

            print(f"Found {len(self.product_urls)} unique product URLs")
            for i, url in enumerate(self.product_urls, 1):
                print(f"{i}. {url}")
            
            return self.product_urls

        except Exception as e:
            print(f"Failed to search products: {e}")
            return []

    def close_driver(self):
        if self.driver:
            self.driver.quit()
    # disconnect_mongo() 

class AmazonSpider(scrapy.Spider):
    name = 'amazon'
    start_urls = ["https://www.google.com"]

    custom_settings = {
        'pipelines': {
            'pipelines.SaveItemPipeline': 300, },
    
        'EXPORT_JSON': True,
        'JSON_FILE': 'amazon_products_pipeline.json', } 


    def __init__(self, pincode, search_query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("Amazon Product Scraper (10 Products)")
        self.pincode = pincode
        self.search_query = search_query
        self.scraped_count = 0
        
        if not self.pincode or not self.search_query:
            print("Both pincode and search query are required!")
            exit(1)

        print(f"Starting scraper for query: '{self.search_query}' with pincode: {self.pincode}")

    def parse(self, response):
        scraper = AmazonSeleniumScrapy(self.pincode, self.search_query)
        
        try:
            print("Setting up Chrome driver...")
            scraper.setup_chrome_driver()
            
            print("Setting up pincode and collecting cookies...")
            if not scraper.setup_pincode_and_get_cookies():
                print("Failed to setup pincode. Exiting...")
                scraper.close_driver()
                return
            
            print("Searching for products...")
            product_urls = scraper.search_and_get_product_urls()
            
            if not product_urls:
                print("No product URLs found. Exiting...")
                scraper.close_driver()
                return
            
            print(f"Found {len(product_urls)} product URLs to scrape")
            
            # cookies = {cookie['name']: cookie['value'] for cookie in scraper.cookies}
            
            scraper.close_driver()
            print("Selenium part completed. Starting product scraping...")
            result = []
            for url in product_urls:
                scraper.driver.get(url)
                resp = scrapy.Selector(scraper.driver.page_source)
                doc = self.parse_product(resp)
                result.append(doc)
            
            for i in result:
                print(i)
                yield i
                # yield scrapy.Request(
                #     url=url,
                #     cookies=cookies,
                #     callback=self.parse_product,
                #     dont_filter=True,
                #     meta={'dont_retry': False}
                # )
                
        except Exception as e:
            print(f"An error occurred during setup: {e}")
            scraper.close_driver()
            return

    def parse_product(self, response):
        self.scraped_count += 1
        print(f"Scraping product {self.scraped_count}: {response.url}")
        
        name = ""
        asin = ""
        mrp = ""
        current_price = ""
        seller = "Amazon"
        
        try:
            name_selectors = [
                '//span[@id="productTitle"]/text()',
                '//h1[@id="title"]/span/text()',
                '//h1//span[@class="a-size-large product-title-word-break"]/text()'
            ]
            
            for selector in name_selectors:
                name_element = response.xpath(selector).get()
                if name_element:
                    name = name_element.strip()
                    break

            asin_selectors = [
                '//th[contains(text(), "ASIN")]/following-sibling::td/text()',
                '//td[contains(text(), "ASIN")]/following-sibling::td/text()',
                '//span[contains(text(), "ASIN")]/following-sibling::span/text()'
            ]
            
            for selector in asin_selectors:
                asin_element = response.xpath(selector).get()
                if asin_element:
                    asin = asin_element.strip()
                    break
            
            if not asin and '/dp/' in response.url:
                asin = response.url.split('/dp/')[1].split('/')[0]

            mrp_selectors = [
                '//span[@class="a-price a-text-price"]/span[@class="a-offscreen"]/text()',
                '//span[@class="a-price a-text-price a-size-medium a-color-secondary"]/span[@class="a-offscreen"]/text()',
                '//span[@data-a-strike="true"]/text()',
                '//span[contains(@class, "a-price-list")]/span[@class="a-offscreen"]/text()'
            ]
            
            for selector in mrp_selectors:
                mrp_element = response.xpath(selector).get()
                if mrp_element:
                    mrp = mrp_element.strip()
                    break

            current_price_selectors = [
                '//span[@class="a-price-whole"]/text()',
                '//span[@class="a-price a-text-price a-size-medium a-color-price"]/span[@class="a-offscreen"]/text()',
                '//span[@class="a-price-symbol"]/following-sibling::span[@class="a-price-whole"]/text()',
                '//span[contains(@class, "a-price-current")]/span[@class="a-offscreen"]/text()'
            ]
            
            for selector in current_price_selectors:
                price_element = response.xpath(selector).get()
                if price_element:
                    current_price = price_element.strip()
                    break

            seller_selectors = [
                '//a[@id="sellerProfileTriggerId"]/text()',
                '//span[contains(text(), "Ships from")]/following-sibling::span/text()',
                '//span[contains(text(), "Sold by")]/following-sibling::span/text()',
                '//div[@id="merchant-info"]//a/text()'
            ]
            
            for selector in seller_selectors:
                seller_element = response.xpath(selector).get()
                if seller_element:
                    seller = seller_element.strip()
                    break

            final_price = current_price if current_price else mrp

            result = {
                "asin": asin,
                "title": name,
                "price": final_price,
                "mrp": mrp,
                "current_price": current_price,
                "seller": seller,
                "url": response.url
            }
            
            print(f"Successfully scraped: {name[:50]}...")
            return result

        except Exception as e:
            print(f"Error parsing product {response.url}: {e}")
            yield {
                "asin": asin,
                "title": name,
                "price": current_price or mrp,
                "seller": seller,
                "url": response.url,
                "error": str(e)
            }


if __name__ == "__main__":
    pincode = input("Enter the delivery pincode: ").strip()
    search_query = input("Enter the product search query (e.g., 'ac', 'laptop', 'mobile'): ").strip()
    
    output_filename = "amazon_products.json"
    process = CrawlerProcess(settings={
        "FEEDS": {
            output_filename: {
                "format": "json", 
                "overwrite": True,
                "indent": 2
            }
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.64 Safari/537.36",
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 1,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "LOG_LEVEL": "INFO"
    })

    process.crawl(AmazonSpider, pincode=pincode, search_query=search_query)
    process.start()
    
    print(f"Scraping completed! Results saved to: {output_filename}")
