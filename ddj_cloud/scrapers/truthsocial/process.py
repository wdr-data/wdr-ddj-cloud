import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString

CURRENT_DIR = Path(__file__).parent


df = pd.read_json(
    CURRENT_DIR / "realDonaldTrump_2025-01-10.jsonl",
    lines=True,
    dtype={
        "id": str,
        # "in_reply_to_id": str,
        # "in_reply_to_account_id": str,
        # "quote_id": str,
        # "in_reply_to": str,
        # "poll": str,
    },
)

print(df.info())
# print(df.sample(10))
"""
Data columns (total 33 columns):
 #   Column                  Non-Null Count  Dtype
---  ------                  --------------  -----
 0   id                      634 non-null    int64
 1   created_at              634 non-null    datetime64[ns, UTC]
 2   in_reply_to_id          0 non-null      float64
 3   quote_id                17 non-null     float64
 4   in_reply_to_account_id  0 non-null      float64
 5   sensitive               634 non-null    bool
 6   spoiler_text            634 non-null    object
 7   visibility              634 non-null    object
 8   language                271 non-null    object
 9   uri                     634 non-null    object
 10  url                     634 non-null    object
 11  content                 634 non-null    object
 12  account                 634 non-null    object
 13  media_attachments       634 non-null    object
 14  mentions                634 non-null    object
 15  tags                    634 non-null    object
 16  card                    173 non-null    object
 17  group                   1 non-null      object
 18  quote                   17 non-null     object
 19  in_reply_to             0 non-null      float64
 20  reblog                  86 non-null     object
 21  sponsored               634 non-null    bool
 22  replies_count           634 non-null    int64
 23  reblogs_count           634 non-null    int64
 24  favourites_count        634 non-null    int64
 25  favourited              634 non-null    bool
 26  reblogged               634 non-null    bool
 27  muted                   634 non-null    bool
 28  pinned                  634 non-null    bool
 29  bookmarked              634 non-null    bool
 30  poll                    0 non-null      float64
 31  emojis                  634 non-null    object
 32  _pulled                 634 non-null    object
dtypes: bool(7), datetime64[ns, UTC](1), float64(5), int64(4), object(16)
"""

KEEP_COLUMNS = (
    "id",
    "created_at",
    "url",
    "content",
    "replies_count",
    "reblogs_count",
    "favourites_count",
)


# df_reblogs = df[df["reblog"].notna()].copy()
# df_reblogs.drop(columns=df_reblogs.columns.difference(list(KEEP_COLUMNS)), inplace=True)
# df_reblogs.to_json(CURRENT_DIR / "realDonaldTrump_2025-01-10_reblogs.jsonl", orient="records", lines=True)

# df_quotes = df[df["quote_id"].notna()].copy()
# df_quotes.drop(columns=df_quotes.columns.difference(list(KEEP_COLUMNS)), inplace=True)
# df_quotes.to_json(
#     CURRENT_DIR / "realDonaldTrump_2025-01-10_quotes.jsonl", orient="records", lines=True
# )

# Remove ReTruths
df = df[df["reblog"].isna()]

# Remove empty content (Only media?)
df = df[df["content"] != "<p></p>"]

# Keep only the columns we need
df.drop(columns=df.columns.difference(list(KEEP_COLUMNS)), inplace=True)

# print(df.sample(10))

re_only_link = re.compile(r"^https?://\S+$")


def process_row(row: pd.Series):
    content_soup = BeautifulSoup(row["content"], features="lxml")
    body = content_soup.find("body")
    assert isinstance(body, Tag)

    content_lines = []

    # This deals with links and multiple paragraphs
    for p in body.find_all("p", recursive=False):
        assert isinstance(p, Tag)

        current_line = []

        for child in p.children:
            if isinstance(child, NavigableString):
                current_line.append(child)
            elif isinstance(child, Tag):
                current_line.append(child.get_text(strip=True))

        content_lines.append("".join(current_line))

    content = "\n".join(content_lines)

    # For some reason, one Truth has a \u2028 character in it
    content = content.replace("\u2028", "").strip()

    if re_only_link.match(content):
        return ""

    return f"""
{content}

URL: {row["url"]}
Date: {row["created_at"].isoformat()}
Replies: {row["replies_count"]}
Reblogs: {row["reblogs_count"]}
Favourites: {row["favourites_count"]}
    """.strip()


processed_rows = df.apply(process_row, axis=1)

processed_rows = [row for row in processed_rows if row]

with open(CURRENT_DIR / "realDonaldTrump_2025-01-10.txt", "w", encoding="utf-8") as f:
    f.write("\n\n\n--- NEXT TRUTH ---\n\n\n".join(processed_rows))
