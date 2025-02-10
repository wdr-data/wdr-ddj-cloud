import datetime as dt
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from enum import Enum

import bs4
import requests

from ddj_cloud.utils.storage import upload_file

BASE_URL = "https://www.whitehouse.gov"


class ListPage(str, Enum):
    ARTICLES = "articles"
    BRIEFINGS_AND_STATEMENTS = "briefings-statements"
    PRESIDENTIAL_ACTIONS = "presidential-actions"


@dataclass
class Article:
    title: str
    url: str
    published_at: dt.datetime
    modified_at: dt.datetime | None
    text: str

    @property
    def text_clean(self) -> str:
        return (
            self.text.replace("\n\n\n\n", "\n")
            .replace(" ", " ")  # nbsp
            .strip()
        )

    def __str__(self) -> str:
        return f"""
{self.title}

Published at: {self.published_at.isoformat()}
Modified at: {self.modified_at.isoformat() if self.modified_at else "N/A"}

{self.text_clean}
        """.strip()


def get_soup(url: str) -> bs4.BeautifulSoup:
    r = requests.get(url)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, features="lxml")


def get_article_hrefs_from_list(soup: bs4.BeautifulSoup) -> Generator[str, None, None]:
    selector = ".wp-block-post-title > a"

    for link in soup.select(selector):
        href = link.get("href")
        assert isinstance(href, str)

        yield href


def _get_all_article_hrefs(url: str) -> Generator[str, None, None]:
    soup = get_soup(url)
    yield from get_article_hrefs_from_list(soup)

    next_link = soup.select_one("link[rel='next']")

    if next_link is not None:
        next_href = next_link.get("href")
        assert isinstance(next_href, str)

        yield from _get_all_article_hrefs(next_href)


def get_all_article_hrefs(page: ListPage) -> Generator[str, None, None]:
    yield from _get_all_article_hrefs(f"{BASE_URL}/{page.value}/")


def _get_meta_tag(soup: bs4.BeautifulSoup, name: str) -> str | None:
    meta_tag = soup.select_one(f"meta[property='{name}']")

    if meta_tag is None:
        return None

    value = meta_tag.get("content")
    assert isinstance(value, str)

    return value


def extract_article_data(soup: bs4.BeautifulSoup) -> Article:
    title = _get_meta_tag(soup, "og:title")
    assert title is not None

    url = _get_meta_tag(soup, "og:url")
    assert url is not None

    published_at_meta = _get_meta_tag(soup, "article:published_time")
    assert published_at_meta is not None
    published_at = dt.datetime.fromisoformat(published_at_meta)

    modified_at_meta = _get_meta_tag(soup, "article:modified_time")
    modified_at = dt.datetime.fromisoformat(modified_at_meta) if modified_at_meta else None

    main_content = soup.select_one("div.entry-content")
    assert main_content is not None

    text = main_content.get_text(separator="\n", strip=True)

    return Article(
        title=title,
        url=url,
        published_at=published_at,
        modified_at=modified_at,
        text=text,
    )


def get_all_articles(page: ListPage) -> Generator[Article, None, None]:
    for href in get_all_article_hrefs(page):
        try:
            soup = get_soup(href)
            yield extract_article_data(soup)
        except Exception:
            print(f"Failed to scrape article at {href}")


def write_articles(articles: Iterable[Article], filename: str):
    article_strs = [str(article) for article in articles]
    article_str = "\n\n\n--- NEXT ARTICLE ---\n\n\n".join(article_strs)

    upload_file(
        article_str.encode("utf-8"),
        filename,
        content_type="text/plain; charset=utf-8",
    )


def run():
    for page in ListPage:
        articles = get_all_articles(page)
        write_articles(articles, f"whitehouse_gov/{page.value}/all_articles.txt")
