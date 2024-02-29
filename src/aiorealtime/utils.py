from urllib.parse import urlparse


def extract_subdomain(url):
    parsed_url = urlparse(url)
    # The netloc attribute gives us the network location part
    netloc = parsed_url.netloc
    # Split the netloc by '.' and get the first part which is typically the subdomain
    subdomain = netloc.split('.')[0]
    return subdomain


def create_realtime_url(supabase_url, api_key):
    supabase_id = extract_subdomain(supabase_url)
    template = "wss://{SUPABASE_ID}.supabase.co/realtime/v1/websocket?apikey={API_KEY}&vsn=1.0.0"
    return template.format(SUPABASE_ID=supabase_id, API_KEY=api_key)


def table_path_to_realtime(table_path: str):
    return 'realtime:'+table_path.replace('.', ':')
