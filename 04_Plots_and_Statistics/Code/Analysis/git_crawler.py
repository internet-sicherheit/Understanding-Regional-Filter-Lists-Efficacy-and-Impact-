import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re


def transform_github_link(link):
    # Remove '/issues/' from links if present
    link = re.sub(r'/issues/', '/', link)

    # Transform 'raw.githubusercontent.com' links to 'github.com' format
    if link.startswith("https://raw.githubusercontent.com/"):
        parts = link.split('/')
        # Format: https://github.com/{user}/{repo}
        if len(parts) >= 5:
            link = f"https://github.com/{parts[3]}/{parts[4]}"
    return link


def crawl_github_links_with_h2_excluding_adblock(url):
    try:
        # Send a GET request to the website
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful

        # Parse the website content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # List to store results as tuples of (h2_text, link)
        github_links_with_h2 = []

        # Find all anchor tags with an href attribute
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            # Convert relative URLs to absolute URLs
            full_url = urljoin(url, href)

            # Only include URLs that contain 'github' and are HTTP/HTTPS
            # Exclude URLs with 'subscribe.adblockplus'
            if (re.match(r'^https?://', full_url) and
                    'github' in full_url.lower() and
                    'subscribe.adblockplus' not in full_url.lower()):
                # Apply transformations to GitHub links
                transformed_url = transform_github_link(full_url)

                # Find the preceding <h2> tag
                h2_tag = a_tag.find_previous('h2')
                h2_text = h2_tag.get_text(strip=True) if h2_tag else "No H2 Found"

                # Store the (h2_text, transformed link) pair
                github_links_with_h2.append((h2_text, transformed_url))

        # Second iteration to remove '/issues/' and eliminate duplicates
        final_links_with_h2 = []
        seen_links = set()  # Track unique URLs

        for h2_text, link in github_links_with_h2:
            # Remove '/issues/' if still present
            cleaned_link = re.sub(r'/issues$', '', link)

            # Only add unique URLs
            if cleaned_link not in seen_links:
                seen_links.add(cleaned_link)
                final_links_with_h2.append((h2_text, cleaned_link))

        return final_links_with_h2

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return []


# Use the provided URL
url = "https://web.archive.org/web/20240521020545/https://easylist.to/pages/other-supplementary-filter-lists-and-easylist-variants.html"
github_links_with_h2 = crawl_github_links_with_h2_excluding_adblock(url)

print("Found unique GitHub links with preceding H2 text (excluding Adblock links and removing duplicates):")
i = 1
for h2_text, link in github_links_with_h2:
    print(f"{i};{h2_text};{link}")
    i += 1
