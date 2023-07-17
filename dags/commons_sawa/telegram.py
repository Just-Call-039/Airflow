def telegram_send(text, token, chat_id, filepath, filename):
    import pandas as pd
    from datetime import datetime
    import requests
    from urllib.parse import urlencode

    filepath = rf'{filepath}/{filename}'

    params = {'chat_id': chat_id, 'text': text}
    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)

    url = base_url + 'sendDocument?' + urlencode(params)
    files = {'document': open(filepath, 'rb')}
    resp1 = requests.get(url, files=files)