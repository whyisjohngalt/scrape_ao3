import gdown
import argparse
import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import os
from google.cloud import storage
from google import auth


URL_DIR = './urldir'
STORY_META_FILENAME = './story_metas.gz'
DELAY = 2.1 # change this to something around 1 to be nice
COMPRESSION_LEVEL = 8
AUTH_FILENAME = './client_secrets.json'
 
def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--id", help = "what is the id of this program")
    parser.add_argument("--client_id", help = "Client ID string")
    parser.add_argument("--client_secret", help = "Client secret string")
    parser.add_argument("--quota_project_id", help = "Quota Project ID string")
    parser.add_argument("--refresh_token", help = "refresh token string")
    parser.add_argument("--type", help = "type string (should be \"authorized user\")")
    
    args = parser.parse_args()
    
    if args.client_id and args.client_secret and args.quota_project_id and args.refresh_token and args.type:
        print("writing to Auth File")
        with open(AUTH_FILENAME, 'w') as auth_file:
          auth_file.write("{\n")
          auth_file.write(f"\"client_id\":\"{args.client_id}\",\n")
          auth_file.write(f"\"client_secret\":\"{args.client_secret}\",\n")
          auth_file.write(f"\"quota_project_id\":\"{args.quota_project_id}\",\n")
          auth_file.write(f"\"refresh_token\":\"{args.refresh_token}\",\n")
          auth_file.write(f"\"type\":\"{args.type}\"\n")
          auth_file.write("}")
    else:
        raise Exception("No auth provided")
    if args.id:
        return int(args.id)
    else:
        raise Exception("No provided ID, try again")
    

def setup():
    print("authenticating to GCP:")
    auth.load_credentials_from_file(AUTH_FILENAME)
    print("authenticated")

# def download_gdrive_files():
#     print("Downloading Gdrive Files")
#     id = "1-5ah0LX__MvPin-YLAABmHW8Y8GI8nbk"
#     gdown.download_folder(id=id, output=URL_DIR)
#     id = "1GTBar5u-WwU-6fdWMqW3EXfJmW45nJGF"
#     gdown.download_folder(id=id, output=URL_DIR)
#     print("Done Downloading Gdrive Files")

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client(project="ao3-proj-1")

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def get_url(id):
    # with open(URL_DIR + f'/{id}.txt', 'r') as url_file:
    with open(f'{id}.txt', 'r') as url_file:
        url = url_file.read().strip()
    return url

def send_request_with_backoff(link):
  result = requests.get(link)
  while result.status_code == 429:
    retry_after= int(int(result.headers['retry-after']) * (4/5)) + 5
    print(f"429 Error, trying again in {retry_after} seconds")
    time.sleep(retry_after)
    result = requests.get(link)
  return result

def get_total_pages_from_soup(soup):
  nav = soup.find('ol', attrs={"role":"navigation"})
  if nav == None:
    total_pages = 1
  else:
    page_links = nav.find_all('a')[:-1]
    total_pages = int(page_links[-1].text)
  return total_pages

def get_num_search_results_from_soup(soup):
  search_results = soup.find_all('h3', attrs={'class':'heading'})[1].text
  search_results = search_results.split(' ')[0]
  search_results = search_results.replace(',','')
  search_results = int(search_results)
  return search_results

def get_story_meta_from_soup(soup):
  meta_list = []
  story_metas = soup.find_all('li', attrs={"role":'article'})
  print(len(story_metas))
  for meta in story_metas:
    _refs = meta.find_all('a')
    if len(_refs) < 2:
      break
    story_title = _refs[0].text
    story_link = _refs[0]['href']
    story_id = story_link.split('/')[-1]
    author = _refs[1].text
    author_link = _refs[1]['href']
    summary = meta.find('blockquote')
    summary = '' if summary is None else summary.text
    language = meta.find('dd', attrs={'class':'language'})
    language = 'UNKNOWN' if language is None else language.text
    word_count = meta.find('dd', attrs={'class':'words'})
    word_count = '0' if word_count is None else word_count.text
    chapters = meta.find('dd', attrs={'class':'chapters'})
    chapters = '0' if chapters is None else chapters.text
    comments = meta.find('dd', attrs={'class':'comments'})
    comments = '0' if comments is None else comments.text
    kudos = meta.find('dd', attrs={'class':'kudos'})
    kudos = '0' if kudos is None else kudos.text
    hits = meta.find('dd', attrs={'class':'hits'})
    hits = '0' if hits is None else hits.text
    meta_list.append([story_title,story_link,story_id,author,author_link,summary,language,word_count,chapters,comments,kudos,hits,meta])
  return meta_list

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project="ao3-proj-1")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def upload_meta_to_cloud(result_filename):
   print("uploading to cloud...")
   upload_blob("ao3_data_repository", STORY_META_FILENAME,result_filename)

# def download_urls_from_cloud()

if __name__ == "__main__":
    id = parse_args()
    print(f"program id = {id}")
    
    setup()
    # download_gdrive_files()

    print("downloading url file")
    download_blob("ao3_data_repository", f"urldir/{id}.txt", f"{id}.txt")
    print("downloaded url file")

    url = get_url(id)
    print(f"URL = {url}")

    result = send_request_with_backoff(url)
    soup = BeautifulSoup(result.text, features="lxml")

    total_pages = get_total_pages_from_soup(soup)
    total_stories = get_num_search_results_from_soup(soup)
    print(f"total pages = {total_pages}, num search results = {total_stories}")

    save_every = 100
    meta_list = []
    stories_read = 0

    for page in range(1,total_pages+1):
        if page != 1: # get the next page
                result = send_request_with_backoff(url + f'&page={page}')
                soup = BeautifulSoup(result.text, features="lxml")
        story_metas = get_story_meta_from_soup(soup)
        meta_list.extend(story_metas)

        if len(meta_list) >= save_every or page >= total_pages:
            stories_read += len(meta_list)
            print(f"Saving story meta info, total amount of stories = {stories_read}/{total_stories}, pages read = {page}/{total_pages}")
            story_links_df = pd.DataFrame(meta_list, columns=["story_title","story_link","story_id","author","author_link","summary","language","word_count","chapters","comments","kudos","hits",'meta'])
            story_links_df.to_csv(STORY_META_FILENAME, mode='a', index=False, compression = {'method':'gzip', 'compresslevel': COMPRESSION_LEVEL})
            meta_list = []
        time.sleep(DELAY)

    upload_meta_to_cloud(f'story_metas_{id}.gz')
