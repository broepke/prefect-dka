"""
Utilities for working with Domains
"""
import requests
import tldextract
import whois
import hashlib
import numpy as np


def extract_domain(url):
    """Utility to extrac the domain name from a URL

    Args:
        url (String): Url of the website for domain extraction

    Returns:
        String: the domain only
    """
    try:
        extracted = tldextract.extract(url)
        return extracted.registered_domain
    except:
        return None


def follow_redirects(url):
    """Checks the redirect chain until it reaches the final URL

    Args:
        url (String): URL to check

    Returns:
        String: the final URL
    """
    session = requests.Session()
    try:
        response = session.get(url, allow_redirects=False, timeout=5)
        while 300 <= response.status_code < 400:
            try:
                url = response.headers["Location"]
            except KeyError:
                # No location header, break out of loop
                break
            response = session.get(url, allow_redirects=False, timeout=5)
    except:
        return url

    return url


def check_website_status(url):
    """Return HTTP status code for a given URL

    Args:
        url (string): URL of site to check

    Returns:
        int: The actual status code or 000 on error
    """
    try:
        response = requests.head(url, timeout=5)
        return response.status_code  # URL is not active
    except:
        return 0  # URL is not active


def is_domain_parked(url):
    """Requests the site and reads the page to look for certain keywords
    that might indicate a parked domain. See: http://python.is

    Args:
        url (String): TLD site

    Returns:
        bool: If the domain appears to be parked
    """
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        html_content = (
            response.text.lower()
        )  # Convert HTML content to lowercase for case-insensitive matching

        parked_keywords = ["parked", "for sale", "under construction"]
        for keyword in parked_keywords:
            if keyword in html_content:
                return True  # Parked domain keywords found

        return False  # Parked domain keywords not found
    except requests.exceptions.RequestException:
        return False  # Error occurred while fetching the webpage


def check_whois_info(domain):
    """Runs WHOIS and returns registration information

    Args:
        domain (String): Full URL or Domain to check

    Returns:
        Dict: WHOIS registration data about the domain
    """
    try:
        w = whois.whois(domain)
        data = {
            "Status": w.status,
            "Creation Date": w.creation_date,
            "Expiration Date": w.expiration_date,
            "Last Updated": w.updated_date,
            "Name Servers": w.name_servers,
        }

        # Handle cases when there are no name servers, then set the value to 0
        try:
            data["Name Servers Count"] = len(data["Name Servers"])
        except:
            data["Name Servers Count"] = 0

        # Convert the lists to strings and escape single quotes
        if type(data["Status"]) == list:
            data["Status"] = ", ".join(data["Status"]).replace("'", "''")

        if type(data["Name Servers"]) == list:
            data["Name Servers"] = ", ".join(data["Name Servers"]).replace("'", "''")

        # In practice we just need the first time reported,
        # Often times you get an array with more than one time
        # we'll extract that and ensure we catch any exceptions
        try:
            data["Creation Date"] = data["Creation Date"][0]
        except:
            data["Creation Date"] = data["Creation Date"]
        try:
            if data["Expiration Date"] == "N/A":
                data["Expiration Date"] = None
            else:
                data["Expiration Date"] = data["Expiration Date"][0]
        except:
            data["Expiration Date"] = data["Expiration Date"]

        try:
            data["Last Updated"] = data["Last Updated"][0]
        except:
            data["Last Updated"] = data["Last Updated"]

        return data
    except Exception as e:
        return {
            "Status": None,
            "Creation Date": None,
            "Expiration Date": None,
            "Last Updated": None,
            "Name Servers": None,
            "Name Servers Count": 0,
        }


def check_homepage_for_updates(url):
    """_summary_

    Args:
        url (String): URL of site to check
        saved_hash (String, optional): The prior has of the website contents.
        If no prior has exists then pass it none and it will return
        the initial hash. Defaults to None.

    Returns:
        bool: Whether or not the content has change
        String: Hash of the content for persistence
    """
    # When there is an error thrown (forbidden)
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        content_hash = hashlib.sha256(response.content).hexdigest()
        return content_hash
    except:
        return "Website Cannot be Scraped"
