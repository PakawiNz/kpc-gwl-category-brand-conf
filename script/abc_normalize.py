import re


def normalize_article_id(id_str: str | None) -> str:
    """Normalizes the article ID by trimming and removing hyphens/underscores."""
    if id_str is None:
        return ""
    return re.sub(r"[-_]", "", str(id_str).strip())


def normalize_brand_id(id_str: str | None) -> str:
    """Normalizes the brand ID by trimming and converting to uppercase."""
    if id_str is None:
        return ""
    return str(id_str).strip().upper()


def normalize_category_id(id_str: str | None) -> str:
    """Normalizes the category ID by trimming, lowercasing, and removing hyphens/underscores."""
    if id_str is None:
        return ""
    return re.sub(r"[-_]", "", str(id_str).strip().lower())


def normalize_costcenter_id(id_str: str | None) -> str:
    if id_str is None:
        return ""
    id_str = id_str.strip()
    if not id_str.isdigit():
        return ""
    return id_str


def normalize_text(text: str | None) -> str:
    if text is None:
        return ""
    return text.strip()
