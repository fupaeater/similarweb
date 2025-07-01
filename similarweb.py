# IMPORTANT USER ACTION REQUIRED:
# The 'cookie' value in the 'headers' variable below needs to be regularly updated.
# To get a fresh cookie:
# 1. Open your web browser (e.g., Chrome).
# 2. Log in to your SimilarWeb Pro account (pro.similarweb.com).
# 3. Open the browser's developer tools (usually by pressing F12).
# 4. Go to the "Network" tab in the developer tools.
# 5. Perform an action on the SimilarWeb site that makes an API request (e.g., analyze a website).
# 6. Look for a request to 'pro.similarweb.com' in the network log.
# 7. Click on the request, and in the "Headers" tab (or "Request Headers"), find the 'Cookie' header.
# 8. Copy the entire string value of the 'Cookie' header.
# 9. Paste this new value to replace the existing cookie string in the 'headers' variable in this script.
# This cookie can expire or be invalidated, leading to errors if not updated.

import json
import time  # Import the time module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

file_input = r'C:\Users\manik\Desktop\facebook-ads\output\merge-csv.com__68632aab0e55b.csv'

current_time_str = datetime.now().strftime('%Y%m%d_%H%M%S')
file_out = f'results/results_{current_time_str}.csv'

ads_df = pd.read_csv(file_input)
headers = {
    'authority': 'pro.similarweb.com',
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json; charset=utf-8',
    'cookie': 'sgID=c0879848-68fd-50fb-9c3c-abf033873e6f; _vwo_uuid=JADBC6979C70445388D19525D9A7FDF09; .DEVICETOKEN.SIMILARWEB.COM=pXs7Mz4E8TILL6zAUUkQTqluf6OhMbwd; __q_state_9u7uiM39FyWVMWQF=eyJ1dWlkIjoiNzQzYzg4MjktMWI5MC00MTNkLTg5YTYtNWU5Y2QzNGNjZDU0IiwiY29va2llRG9tYWluIjoic2ltaWxhcndlYi5jb20iLCJtZXNzZW5nZXJFeHBhbmRlZCI6ZmFsc2UsInByb21wdERpc21pc3NlZCI6ZmFsc2UsImNvbnZlcnNhdGlvbklkIjoiMTI1Nzc3ODQ5NzUyMjEzMjE1MCJ9; bm_sz=3FAF7AB197701C4E13446B9AEDF058A6~YAAQDHkzuCRRX/SMAQAArLZq9haWFOHqmhYJaxkuMlVI6gQJz85iC2eUpvjXxgGM29/uBOWkxIAM0djgKRy3raXuWv6eNR4bpZVVwn3spkBKyBBE+/liwtwqmmAvb+tp1DPncq8mS0Xnb8LWHbIwArhWfFqC0sT4c6cf0l2IZFgrmsyAQZyDZoZrBlShilVQnZ/Rg6Sit1GGI9ROrdn5jaDxFZwn5W5y0e2amEIppjeC/MfrOgTQeA7SEPFTBtF7MbcrgWqc7G2J3ZIKfEcZMTofKhc8fK/V3I0Yd6YgPRr/tPMvIdW8~4405559~3687219; .SGTOKEN.SIMILARWEB.COM=mxGVLgVQawlM0O4DTz4DZGqjtqqNszA86jqG5qAUtiXTxEOpjBWNYh5JFBJ4qcXc7OBAWOBhw8cw4rwPPu4UdibjAk7wgVPhpsicMaItrfbYHdo1yGUg9kCMZgeEbzL4gGNhGz2Qc4_PlbnNq0Uaa9bBBfe5mYGOOTIk9NJh2f51FzW_iH3HB036Z6jy6OcWH5ehDecGMD-mRc0bZq7dczweGQ7VbIbsi9jUFtxYvmgRtivhxbYmjCQNk5K_5vjKwDc27s9Jmn2wwsFpupM_kDQ5QG1dMzli-MlqsU-STY7yaAwwOP60THOl8qdnVXldTnTVL_-tba5I7jW4tYBeWdILUlHxbAkhfVFWT0e9am-3yCmjKZsssDLLQWavif-pQDIr9_pgRKZ1IUD9fYFX-qPeDehihGpqaHWP-E2lZ6YcFdlXZxRJwi-2SmRop9-K; locale=en-us; RESET_PRO_CACHE=False; bm_mi=5B1BE229ADDA859ADB3CC9AAAF358435~YAAQL3kzuDInjuyMAQAACN959hZ9pWqZzYh4Yegm6VfxM/t1DKhMv/ERFLl7WEwsF0q0IRFsjpuwTnwtNdwK9U7UiIKBvV4+HAIToFBGC/eQo58u+yee/gQRTTyg/NgoBYcwGmV6aJgrylhQP8X/rqguSM4lwHWk5ShtNy4G8ezCQrnMqiWwu/vClVN6nUpywWzcHehcYciN8C+tHR2KxS8wLi8WF0BEoc3YI/WSPGcDDmoy3aFNRNVv/RdM4xpr4JZKCpYahwtRSyWDGotltLtZtuJ0M1Eju5zduY8SotaQxejoqm83YYUrCr21EKwhJg==~1; bm_sv=0D1DDBEF72A89F9BCCE3F8A2E9B896E5~YAAQL3kzuDsnjuyMAQAAEPt59hZ2HpwfgDtG5o2W7cgMYWZ7+lKgbNlVwA3ZDbAskXkR0pIc6tVkhjYww7hRJzMu/LTQiLhFmMxxHIMDcRPDGe0rpHTcV4OcPMln3YMvaiXO7stb95uekeYEWYbVgLrS+LKRZ5GrKGo2RtErpikk5l9jqQ1sxMLXOD6R9fibTpjOnyqTKMwO9rnHF4jlOGkNwLC9KGKaXCAcffKuf441E4Yk5rP2Nnh5Z+MWXYtyTxOSaCI=~1; ak_bmsc=879FB4BB74077DF3CF6B67053B093B73~000000000000000000000000000000~YAAQL3kzuEUnjuyMAQAANCd69hYjmVBAZOHnjiQGkkirrDt5yYMcQFhJGXD6j9ChLujy7VScjgribGPOAr+4KSHagjjmT8odTLdo7qz4GErUK5TfNTFD6lH7W8ET6nM/9fZAy0L+4zSQUGgwaWhUeoFL18Iglw1BGE59sXjLl9i/aEsJBkbyuq1OMFeesy+8+GO17isUqON1IAve9pb/QQtO9gbfhcmfTN0deW5SihL/vo2HxNhHW9zMB5tdahr38JddGpLbHAmSudVS1YyOP71XfbXwnYrafCjI0SLcOrbtSzosx0qgB7Lw+i4opVQdpw97YhcGFmfFGM/Z6ZUCjTv55hCPq9O1xazgL1RI+aFSbUIr5rnddeLvdS2G1YY7Rw1BftY4UQ1xKW9saRmEjmyiwBm9/21KD+pL0AKAZe0E5Cs=; __zlcmid=1JlmPe6T93KiGBt; _abck=29CB137377643690358742C1BA1711D7~0~YAAQi0J1aPnpqWOMAQAA5mSr9gsNccvXtJh1s1r3EyjA2oXwLYiwcweotUmPokWIb9JOawt4eaVVc4CKqNj3pdreGZeG9Cqnpr/Zmt/mckb1kcZswAK6Gwqp74voVU9GfUcOLh9ohf7Ie1TaQZWzNZHfpDIfwdifG9GXTgHEvg8qe1jcr9sUaHN73a4v5XpZmTO5zCZBXagzVEn8++GkgdmTIz5xDPNbFAUIiKwrkULFjBezpbG58V2pTQ4IncrXO/zEBKPYEUD+xY+WtOIG8uEKujnKtDmZFTEhjLSQzs5wzW/8zeTyWTLg9KmQGzrhpm5mWuuCI8zSpm4LW3Zu5BT/ev2w14AnqQejEDyyPFb2xHlIkX/Y4flAkL+VSg8bl8ScfP3Le8bvnGWCihAwPIAjNbf0HRdn/5PC/A==~-1~-1~-1; _abck=29CB137377643690358742C1BA1711D7~-1~YAAQM3nKF1bYk+yMAQAAZ9LM9guHZ67fSgarriSBUeWOLXkdXxcRHnjsEAtvsgDaZCuLCzvCTYBZXPH9E/oB0f4VTYAaZgtR2weuRLZj4gdAsVV60198p12N1BAIaeHDoazYfcltganlxHsKaWnbBMtmYeou4Zp3RgNUi9ddVSNMiZdZED2gZwER2RuOAJdoQHy0MtLRy9aYuqcAViovAicQWhtoQuhq4/Yw0J1gWw4UE1V3Q9IopXOC4/lop3wEYGp6c0ff+umU4PccwdndSjC7hOqjRIKllV+JDofkFBeFGkWGHRoeLyAkWaZIZWzm8ejS3yK37CqKeVwyPxDw2lgoeGZx2hY/Wacz+0q9+53HBp+OPzT1pfsgtM1118QXDrli9PEwnqMUO/woGe8o7ghg/4MWjvl97I9+Iw==~0~-1~-1; bm_sv=8FBDB7B5F18A4950BD112C93B4C1FA1D~YAAQi0J1aDnqqWOMAQAA6xmv9hbrwEWwpULjpzR8mv4yHX5eWn3HX94juaj+0sCu6CujsRpqJR7WP8jhJgtNbfk/Y4jjV4y7ZYCsktVdHtPT585nt4jBEY+OZTG2nmYjI/mURWmJm5vObYx9Yam4MwEXVcCCDEtYyM2tTHVAlTI+YWWBOj4FOAHZyKxrXHQLV/ENa+8wE4lhBQAeUxBYqVfc/qrl2DYeJpObYpMbH2q/kXGvNqMVO8xwdOTLrLEnvBzz8Q==~1; bm_sz=4995B5D48012B6C487F5D06D42B014AC~YAAQLXkzuOWKc9CMAQAAqP2X9hahtkQ0Ooxf/L6PeD3FOUaAsU26zgm1dWgoPRNuKEiOy+qGdCsRPZWnlUw0BWXPeKeN3xYKpTKSJe+dF+pClFpJ9HRijibuQDZbZZGr2MfsMv5/NAu8JElaHfk0FTbUnbBjGOy/sOoqKHPYaLpOlWPEIby10Ft/vbj2svWrEI7G8oGteEWPC/cG1HtF0EHMKNhFPuENopIr5gE4XzpICtFdndIxZgfA+0P3Iq0IuRxPhsHbMzcDv5EeHwWwKwQvo7yoCs46SS4gcRWhqgwaKfFaFTQh~3290691~3683122; sec_cpt=A331ACDED45C518865D1235DA9F474FE~1~YAAQi0J1aDrqqWOMAQAA6xmv9gozgHYix+zW+t6zFXACDUCLuxAj4GHazc6lUXcvgcyKEc5ExEKUedF/WXAkRfrb4rMjHkxe8jw/wZP3KJy4OM9CS7FPbykyF7xoHMVptDN6SSMmzVnd1kP1IpyXc4fx2RekPPq+d+Aykbg2niX3yrRngV/x7zwkXK4hhaB1jDhNhsgHbwlLdhda91mmM3VtieYDwoI4UyEz4kBJtfeFrSTHhLq2E4EPFbuSuVOe5eoJhXNuTNhcibHiuQijY859KSlKpoK6YvZG4UelzN0IebWWqO4a3KwoOtAY0PkVxaxVSQ7IUl1BTkSDIvhJJVyFXwg3RKQ39k8JUsZsm5sWU4LK5hV6Ij7xDbpeGbPlps2LV8pLD7W18OCLTcHoEJvQPan81zTnmymIkfnrb3sQMMCL/QGY7KRjjVC10ieuS9BwWYfUpbIP7dGT1HZb0Exjxd01uIgGeqbZiXmUhoarAR7uKp8Vf+4unkLlJmP+SMDE/6++Zxgtq2rp9uBgMkelK3GJuU1I/E1lj7zfjOr81j3QQG3hAMzS6LC7Md15O+jIPvVZYqK5E82dF2ejINwQscaHdaLrhEmbyt97jkzTqkg8CuN4hdNz/+jzTmiOH3dVS/7z53v4VMVUbXjGFwrtgM9t/dqu',
    'referer': 'https://pro.similarweb.com/',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
    'x-sw-page': 'https://pro.similarweb.com/#/digitalsuite/websiteanalysis/overview/website-performance/*/999/28d?webSource=Total&key=pornhub.com',
    'x-sw-page-view-id': '7fd59136-4039-47cb-a11a-68ea497d92cb'
}


def create_session():
    retry_strategy = Retry(
        total=3,  # Total number of retries to allow
        status_forcelist=[429, 500, 502, 503, 504],  # A set of HTTP status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Allow retries for these HTTP methods
        backoff_factor=1,  # Wait 2 seconds between retries
        raise_on_status=False,  # Do not raise an exception for status codes in status_forcelist
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def check_for_shopify(url):
    try:
        # Use a new session or the global one, ensuring timeout is set.
        # Using a new session for simplicity here, but for many calls, reusing the global session is better.
        res = requests.get(url, timeout=30)
        res.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        return 'cdn.shopify.com' in res.text
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error checking Shopify for {url}: {e}")
        return False
    except requests.exceptions.Timeout as e:
        print(f"Timeout checking Shopify for {url}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Request exception checking Shopify for {url}: {e}")
        return False
    except Exception as e:  # Catch any other unexpected errors
        print(f"An unexpected error occurred while checking Shopify for {url}: {e}")
        return False


def get_shopify(url):
    is_shopify = check_for_shopify(url)
    ads_df.loc[ads_df['ad_url'] == url, 'shopify'] = is_shopify


ads_df
unique_shop_urls = ads_df['shop_url'].unique()
traffic_data = {}
session = create_session()
ads_df['shopify'] = False

# Batch processing settings
processed_count = 0
batch_size = 50
batch_delay_seconds = 30

for shop_url in unique_shop_urls:
    try:
        traffic_data[shop_url] = None

        # Individual request delay
        time.sleep(1)  # Sleep for 1 second (can be adjusted or removed)

        api_url = f"https://pro.similarweb.com/widgetApi/WebsiteOverview/EngagementVisits/Graph?country=999&from=2023%7C12%7C11&to=2024%7C01%7C07&timeGranularity=Weekly&ShouldGetVerifiedData=false&includeSubDomains=true&isWindow=true&keys={shop_url}%2Cxnxx.com%2Cxhamster.com%2Cyouporn.com%2Cxvideos.com&webSource=Total&latest=28d"
        print(f"Fetching traffic data for domain: {shop_url} (Count: {processed_count + 1})")
        response = session.get(api_url, headers=headers)
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                first_key = next(iter(data['Data']))
                last_value = data['Data'][first_key]['Total'][0][-1]['Value']
                print(f"Successfully fetched traffic for {shop_url}: {last_value}")
                if isinstance(last_value, float):
                    last_value = int(last_value)
                traffic_data[shop_url] = last_value
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for {shop_url}: {e}")
                print(f"Response text: {response.text[:500]}...")  # Log first 500 chars
                traffic_data[shop_url] = None  # Or an error indicator
            except KeyError as e:
                print(f"KeyError accessing data for {shop_url}: {e}")
                print(f"Response data structure: {data}")
                traffic_data[shop_url] = None  # Or an error indicator
            except Exception as e:  # Catch any other unexpected errors during data processing
                print(f"Unexpected error processing data for {shop_url}: {e}")
                traffic_data[shop_url] = None
        else:
            print(
                f"API request failed for {shop_url} with status code {response.status_code}: {response.text[:500]}...")
            traffic_data[shop_url] = None  # Or an error indicator
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {shop_url}: {e}")
        traffic_data[shop_url] = None  # Or an error indicator
    except Exception as e:  # Catch any other unexpected errors during the request phase
        print(f"An unexpected error occurred while fetching data for {shop_url}: {e}")
        traffic_data[shop_url] = None
    finally:
        processed_count += 1
        if processed_count % batch_size == 0:
            print(f"Processed {processed_count} URLs. Pausing for {batch_delay_seconds} seconds...")
            time.sleep(batch_delay_seconds)

ads_df['traffic'] = ads_df['shop_url'].map(traffic_data)
with ThreadPoolExecutor(max_workers=50) as executor:
    futures = [executor.submit(get_shopify, url) for url in ads_df['ad_url'].unique()]

for future in futures:
    future.result()


def clean_illegal_chars(dataframe):
    # Define illegal characters that Excel does not support
    illegal_chars = [chr(i) for i in range(0, 32)]  # ASCII control characters 0x00-0x1F
    illegal_chars.remove('\t')  # Tab might be used intentionally in text
    illegal_chars.remove('\n')  # Newline might be used intentionally in text
    illegal_chars.remove('\r')  # Carriage return might be used intentionally in text

    # Convert DataFrame columns to string and replace illegal characters
    for col in dataframe.columns:
        if dataframe[col].dtype == object:  # Typically 'object' type columns are strings
            for char in illegal_chars:
                dataframe[col] = dataframe[col].str.replace(char, '', regex=False)
    return dataframe


ads_df = clean_illegal_chars(ads_df)
ads_df.sort_values(by=['shopify', 'traffic'], inplace=True, ascending=False)
ads_df.to_csv(file_out, index=False)
