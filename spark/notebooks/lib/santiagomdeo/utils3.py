import urllib.parse

def normalize_url(raw_url):
    if raw_url is None:
        return None

    try:
        parsed = urllib.parse.urlparse(raw_url)

        # Lowercase hostname
        netloc = parsed.netloc.lower()

        # Remove fragment (#...)
        fragment = ""

        # Parse and clean query parameters
        query_params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)

        blocked = {"utm_source", "utm_medium", "utm_campaign", "gclid", "fbclid"}

        clean_params = [(k, v) for (k, v) in query_params if k not in blocked]
        clean_params.sort()  # sort to remove duplication differences

        clean_query = urllib.parse.urlencode(clean_params)

        normalized = urllib.parse.urlunparse((
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            clean_query,
            fragment
        ))

        # Remove trailing slash
        if normalized.endswith("/") and len(normalized) > len(parsed.scheme)+3:
            normalized = normalized[:-1]

        return normalized

    except Exception:
        return raw_url
