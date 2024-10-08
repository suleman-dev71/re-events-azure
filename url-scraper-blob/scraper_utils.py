from excluded_words import excluded_words
from urllib.parse import urlparse

def contains_excluded_words(string):
    return any(word in string for word in excluded_words)


def filter_link(link):
    if (not link.endswith(("png", "jpg", "jpeg", "pdf", "mp4", "mp3"))) and (
        not contains_excluded_words(link.lower())
    ):
        return True



def extract_domain(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if domain.startswith("www."):
        domain = domain[4:]

    return domain

