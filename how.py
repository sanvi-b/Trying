import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from random import uniform
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_title(soup):
    try:
        title = soup.find("span", attrs={"id": "productTitle"}) or soup.find("h1", attrs={"id": "title"})
        return title.text.strip() if title else ""
    except Exception as e:
        logger.error(f"Error extracting title: {e}")
        return ""

def get_selling_price(soup):
    try:
        price = soup.find("span", attrs={'class': 'a-price-whole'}) or soup.find("span", attrs={'class': 'a-price'})
        if price:
            return price.text.strip().replace(".", "").replace(",", "").replace("₹", "")
        return ""
    except Exception as e:
        logger.error(f"Error extracting selling price: {e}")
        return ""

def get_MRP(soup):
    try:
        selectors = [{'class': 'a-size-small aok-offscreen'}, {'class': 'a-text-price'}]
        for selector in selectors:
            price = soup.find("span", attrs=selector)
            if price:
                return price.text.strip().replace("M.R.P.: ₹", "").replace("₹", "").replace(",", "")
        return ""
    except Exception as e:
        logger.error(f"Error extracting MRP: {e}")
        return ""

def get_discount(soup):
    try:
        discount = soup.find("span", attrs={'class': 'savingsPercentage'})
        return discount.text.strip().replace('-', '').replace('%', '') if discount else ""
    except Exception as e:
        logger.error(f"Error extracting discount: {e}")
        return ""

def get_rating(soup):
    try:
        rating_selectors = [{'class': 'a-icon a-icon-star'}, {'class': 'a-icon-alt'}]
        for selector in rating_selectors:
            rating = soup.find("i", attrs=selector) or soup.find("span", attrs=selector)
            if rating:
                return rating.text.strip().split()[0]
        return ""
    except Exception as e:
        logger.error(f"Error extracting rating: {e}")
        return ""

def get_reviews(soup):
    try:
        reviews = soup.find("span", attrs={'id': 'acrCustomerReviewText'})
        return reviews.text.strip().split()[0].replace(",", "") if reviews else ""
    except Exception as e:
        logger.error(f"Error extracting reviews: {e}")
        return ""

def get_review_text(soup):
    try:
        review_element = soup.find("div", attrs={'data-hook': 'review-collapsed'})
        if review_element:
            return review_element.text.strip()
        review_summary = soup.find("div", attrs={'class': 'a-expander-content reviewText'})
        return review_summary.text.strip() if review_summary else "No review available"
    except Exception as e:
        logger.error(f"Error extracting review text: {e}")
        return "No review available"

def scrape_amazon_products(search_url, max_products=50):
    base_url = "https://www.amazon.in"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }

    try:
        session = requests.Session()
        response = session.get(search_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", attrs={'class': 'a-link-normal s-no-outline'})

        if not links:
            logger.warning("No product links found on the search page")
            return pd.DataFrame()

        d = {
            "title": [], "selling_price": [], "MRP": [],
            "discount": [], "rating": [], "reviews": [],
            "availability": [], "url": [], "scrape_datetime": [],
            "review_text": []
        }

        for link in links[:max_products]:
            product_url = link.get('href')
            if not product_url:
                continue
            if not product_url.startswith('http'):
                product_url = base_url + product_url

            try:
                logger.info(f"Scraping product: {product_url}")
                time.sleep(uniform(1, 3))
                response = session.get(product_url, headers=headers)
                response.raise_for_status()

                product_soup = BeautifulSoup(response.content, "html.parser")
                current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                d['title'].append(get_title(product_soup))
                d['selling_price'].append(get_selling_price(product_soup))
                d['MRP'].append(get_MRP(product_soup))
                d['discount'].append(get_discount(product_soup))
                d['rating'].append(get_rating(product_soup))
                d['reviews'].append(get_reviews(product_soup))
                d['availability'].append("Available")  # assuming available as default
                d['url'].append(product_url)
                d['scrape_datetime'].append(current_datetime)
                d['review_text'].append(get_review_text(product_soup))

            except Exception as e:
                logger.error(f"Error scraping product {product_url}: {e}")
                continue

        df = pd.DataFrame.from_dict(d)
        df['title'] = df['title'].replace('', pd.NA)
        df = df.dropna(subset=['title'])

        return df

    except Exception as e:
        logger.error(f"Error in main scraping process: {e}")
        return pd.DataFrame()

def create_reviews_csv(df):
    try:
        reviews_df = df[['title', 'scrape_datetime', 'review_text', 'availability']].copy()
        reviews_df = reviews_df[reviews_df['review_text'] != "No review available"]
        reviews_df.to_csv("reviews.csv", index=False)
        logger.info(f"Successfully created reviews.csv with {len(reviews_df)} reviews")
    except Exception as e:
        logger.error(f"Error creating reviews CSV: {e}")

if __name__ == '__main__':
    search_url = "https://www.amazon.in/s?k=earphones&crid=23H19CC51YB96&sprefix-earphone%2Caps%2C228&ref=nb_sb_noss_2"
    df = scrape_amazon_products(search_url)

    if not df.empty:
        df.to_csv("amazon_scraped_data.csv", header=True, index=False)
        logger.info(f"Successfully scraped {len(df)} products")

        create_reviews_csv(df)
    else:
        logger.error("No data was scraped")
