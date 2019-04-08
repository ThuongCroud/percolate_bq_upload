import json
import logging
import os
import socket
import sys
import time
import unicodedata
from pprint import pprint
import requests
from requests.exceptions import HTTPError
from datetime import datetime, timedelta
from dateutil.parser import parse as dt_parse



class PercolateAPIError(BaseException):
    pass


def count_calls(fn):
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        return fn(*args, **kwargs)
    wrapper.calls = 0
    wrapper.__name__ = fn.__name__
    return wrapper


def relative_insert(path):
    """
    Insert a relative path into sys path.
    """
    abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), path))
    sys.path.insert(0, abspath)


PERC_HEADERS = {'Content-Type': 'application/json'}
PERCOLATE_BASE_URL = os.environ.get(
    "PERCOLATE_BASE_URL") or 'https://percolate.com/api'


def get_user_id_from_api_key(api_key):
    json_data = get_object(api_key, '/v5/me')
    d = json_data['data']
    return get_id_from_uid(d['id'])


def get_all_objects(api_key, url, params=None, page_limit=100):
    """
    Wrapper for pagination of Percolate API

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- API endpoint URL without Percolate base
    params -- dictionary of URL encoded parameters
    """
    if params is None or not isinstance(params, dict):
        params = {}
    result = []
    headers = PERC_HEADERS
    headers['Authorization'] = api_key
    if not url.endswith('?'):
        url = url + '?'
    if not url.startswith('/'):
        url = '/' + url

    completed = False
    while not completed:
        offset = 0
        limit = page_limit
        total = page_limit
        # page through
        while offset < total:
            url_params = {'limit': limit, 'offset': offset}
            url_params.update(params)

            json_data = get_object(api_key, url, url_params)
            try:
                total = json_data['pagination']['total']
            except KeyError:
                total = json_data['meta']['total']

            if json_data:
                result.extend(json_data['data'])

            offset += limit
            if offset >= total:
                completed = True
    return result


@count_calls
def get_object(api_key, url, params=None):
    """
    Wrapper for single object GET of Percolate API (no pagination)

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- API endpoint URL without Percolate base
    params -- dictionary of URL encoded parameters
    """
    if not params:
        params = {}
    headers = PERC_HEADERS
    headers['Authorization'] = api_key
    if not url.endswith('?'):
        url = url + '?'
    if not url.startswith('/'):
        url = '/' + url
    api_url = PERCOLATE_BASE_URL + url
    result = None
    url_params = {}
    url_params.update(params)

    retry = 0
    while retry < 10:
        try:
            response = requests.get(api_url, params=url_params,
                                    headers=headers, timeout=60)
            response.raise_for_status()
            result = response.json()
            # urlfetch.set_default_fetch_deadline(60)
            # response = urlfetch.fetch(
            #     api_url + urllib.urlencode(url_params),
            #     headers=headers, deadline=None
            # )
            # result = json.loads(response.content)
            if response.status_code != 503:
                break
        except HTTPError as e:
            status_code = e.response.status_code
            if isinstance(status_code, int) and 400 <= status_code < 500:
                if status_code == 429:
                    print('RATE MAX HIT')
                    time.sleep(60)
                    continue
                # print('{}\n{}\n{}'.format(e, url_params, headers))
                raise PercolateAPIError(status_code, result, api_url,
                                        url_params, headers)
        except (requests.exceptions.RequestException, ValueError, HTTPError,
                socket.timeout, socket.error) as e:
            logging.error(e)
            logging.error("retrying...")
            if retry == 9:
                print(result)
                raise
            time.sleep(1)
        retry += 1

    if 'errors' in result:
        raise PercolateAPIError(result, api_url, url_params, headers)
    return result


@count_calls
def get_asset_download_url(api_key, asset_uid):
    url = 'https://percolate.com/pam/api/v5/asset/{}/download'
    headers = {'Content-Type': 'application/json', 'Authorization': api_key}
    # params = {'disposition': 'inline'}
    params = {}
    response = None

    retry = 0
    while retry < 10:
        try:
            response = requests.get(url.format(asset_uid), params=params,
                                    headers=headers, allow_redirects=False)
            response.raise_for_status()
            break
        except HTTPError as e:
            status_code = e.response.status_code
            if isinstance(status_code, int) and 400 <= status_code < 500:
                if status_code == 429:
                    print('RATE MAX HIT')
                    time.sleep(60)
                    continue
                # print('{}\n{}\n{}'.format(e, url_params, headers))
                raise PercolateAPIError(
                    status_code, asset_uid, headers, params)
        except (requests.exceptions.RequestException, ValueError, HTTPError,
                socket.timeout, socket.error) as e:
            logging.error(e)
            logging.error("retrying...")
            if retry == 9:
                print(response.text)
                raise
            time.sleep(1)
        retry += 1

    download_url = response.headers['Location']
    return download_url


def get_id_from_uid(uid):
    """
    Returns numeric ID of an object from its unique ID
    Example: license:12345 returns 12345

    Keyword Arguments:
    uid -- Unique object ID
    """
    return uid.split(':', 1)[1]


def upload_asset(api_key, url, scope_uid, folder_uid='folder:primary'):
    """
    Uploads URL asset into Percolate

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- URL of asset to upload
    scope_uid -- UID of the scope of the asset (license, brand, account)
    """
    data = {
        'type': 'url',
        'destination_id': folder_uid,
        'scope_id': scope_uid,
        'ext': {'url': url},
        'upload_state': 'preparing'
    }
    # Kick off the upload
    result = post_object(api_key, '/v5/upload/', data)
    # Send back the upload UID for polling
    upload_id = result['id']
    asset_uid, is_dupe = _check_asset_upload_status(api_key, upload_id)
    return asset_uid, upload_id, is_dupe


def _check_asset_upload_status(api_key, upload_uid, give_up_seconds=0):
    """
    Returns status checks of uploads into Percolate

    Keyword Arguments:
    api_key -- API key of the user for authentication
    upload_uid -- Upload UID
    give_up_seconds -- seconds to wait for response if not ready
    """
    give_up = False
    give_up_at = time.time()
    asset_id = None
    if give_up_seconds > 0:
        give_up = True
        give_up_at = time.time() + give_up_seconds
    status = ''
    is_dupe = False
    while status not in ('ready',):
        # logging.debug("Checking status of upload {}...".format(upload_uid))
        # Poll the endpoint
        response = get_object(api_key, '/v5/upload/' + upload_uid)
        status = response['data']['status']
        if status in ('ready', 'complete', 'duplicate'):
            asset_id = response['data']['asset_id']
            if status == 'duplicate':
                is_dupe = True
            if asset_id is not None:
                    break
        elif status == 'error':
            raise PercolateAPIError('Error creating asset from upload.')
        else:
            if give_up and time.time() > give_up_at:
                logging.warning("Giving up...")
                return asset_id, is_dupe
            else:
                logging.debug("Asset not ready. Waiting 3 seconds...")
                time.sleep(3)

    logging.debug("Upload {} Asset ready: {}".format(upload_uid, asset_id))
    return asset_id, is_dupe


@count_calls
def post_object(api_key, url, data=None, verbose=False):
    """
    Wrapper for POST Calls of Percolate API

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- API endpoint URL without Percolate base
    data -- dictionary of payload data
    """
    if not data:
        data = {}
    headers = PERC_HEADERS
    headers['Authorization'] = api_key
    if not url.endswith('?'):
        url = url + '?'
    if not url.startswith('/'):
        url = '/' + url
    api_url = PERCOLATE_BASE_URL + url
    result = None
    response = None

    retry = 0
    while retry < 10:
        try:
            if verbose:
                print(api_url)
                print(json.dumps(data, sort_keys=True, indent=4))
                pprint(headers)
            response = requests.post(api_url, data=json.dumps(data),
                                     headers=headers, timeout=60)
            result = response.json()
            response.raise_for_status()
            # urlfetch.set_default_fetch_deadline(60)
            # response = urlfetch.fetch(
            #     api_url,
            #     method='post',
            #     headers=headers, deadline=None,
            #     payload=json.dumps(data)
            # )
            # result = json.loads(response.content)
            break
        except HTTPError as e:
            status_code = e.response.status_code
            if isinstance(status_code, int) and 400 <= status_code < 500:
                if status_code == 429:
                    print('RATE MAX HIT')
                    time.sleep(60)
                    continue
                # print('{}\n{}\n{}'.format(e, url_params, headers))
                raise PercolateAPIError(status_code, result, api_url,
                                        json.dumps(data), headers)

        except json.decoder.JSONDecodeError as json_error:
            error_status_code = json_error.response.status_code
            if isinstance(error_status_code, int) and error_status_code == 502:
                continue
            print('JSON error: {}'.format(json_error))
            status_code = response.status_code
            raise PercolateAPIError(status_code, result, api_url,
                                    json.dumps(data), headers)
        except (requests.exceptions.RequestException, ValueError, HTTPError,
                socket.timeout, socket.error) as e:
            logging.error(e)
            logging.error("retrying...")
            if retry > 0:
                # TODO - Count retries
                # count_calls()

                pass
            if retry == 9:
                print(result)
                raise
            time.sleep(1)
        retry += 1

    if 'errors' in result:
        print(response.request.body)
        raise PercolateAPIError(result, api_url, data, headers)
    if 'data' in result:
        return result['data']
    return result


@count_calls
def delete_object(api_key, url):
    """
    Wrapper for POST Calls of Percolate API

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- API endpoint URL without Percolate base
    data -- dictionary of payload data
    """
    headers = PERC_HEADERS
    headers['Authorization'] = api_key
    if not url.endswith('?'):
        url = url + '?'
    if not url.startswith('/'):
        url = '/' + url
    api_url = PERCOLATE_BASE_URL + url
    result = None
    response = None

    retry = 0
    while retry < 10:
        try:
            response = requests.delete(api_url, headers=headers, timeout=60)
            result = response.status_code
            response.raise_for_status()
            # urlfetch.set_default_fetch_deadline(60)
            # response = urlfetch.fetch(
            #     api_url,
            #     method='post',
            #     headers=headers, deadline=None,
            #     payload=json.dumps(data)
            # )
            # result = json.loads(response.content)
            break
        except HTTPError as e:
            status_code = e.response.status_code
            if isinstance(status_code, int) and 400 <= status_code < 500:
                if status_code == 429:
                    print('RATE MAX HIT')
                    time.sleep(60)
                    continue
                # print('{}\n{}\n{}'.format(e, url_params, headers))
                raise PercolateAPIError(status_code, result, api_url,
                                        json.dumps({}), headers)
        except (requests.exceptions.RequestException, ValueError, HTTPError,
                socket.timeout, socket.error) as e:
            logging.error(e)
            logging.error("retrying...")
            if retry > 0:
                # TODO - Count retries
                # count_calls()

                pass
            if retry == 9:
                print(result)
                raise
            time.sleep(1)
        retry += 1

    if result != 204:
        print(response.request.body)
        raise PercolateAPIError(result, api_url, headers)
    return True


@count_calls
def put_object(api_key, url, data=None):
    """
    Wrapper for PUT Calls of Percolate API

    Keyword Arguments:
    api_key -- API key of the user for authentication
    url -- API endpoint URL without Percolate base
    data -- dictionary of payload data
    """
    if not data:
        data = {}
    headers = PERC_HEADERS
    headers['Authorization'] = api_key
    if not url.endswith('?'):
        url = url + '?'
    if not url.startswith('/'):
        url = '/' + url
    api_url = PERCOLATE_BASE_URL + url
    result = None

    retry = 0
    while retry < 10:
        try:
            response = requests.put(api_url, data=json.dumps(data),
                                    headers=headers, timeout=60)
            result = response.json()
            response.raise_for_status()
            # urlfetch.set_default_fetch_deadline(60)
            # response = urlfetch.fetch(
            #     api_url,
            #     method='put',
            #     headers=headers, deadline=None,
            #     payload=json.dumps(data)
            # )
            # result = json.loads(response.content)
            break
        except HTTPError as e:
            status_code = e.response.status_code
            if isinstance(status_code, int) and 400 <= status_code < 500:
                if status_code == 429:
                    print('RATE MAX HIT')
                    time.sleep(60)
                    continue
                # print('{}\n{}\n{}'.format(e, url_params, headers))
                raise PercolateAPIError(status_code, result, api_url,
                                        json.dumps(data), headers)
        except json.decoder.JSONDecodeError as json_error:
            print('JSON error: {}'.format(json_error))
            status_code = response.status_code
            raise PercolateAPIError(status_code, result, api_url,
                                    json.dumps(data), headers)
        except (requests.exceptions.RequestException, ValueError, HTTPError,
                socket.timeout, socket.error) as e:
            logging.error(e)
            logging.error("retrying...")
            if retry == 9:
                print(result)
                raise
            time.sleep(1)
        retry += 1

    if 'errors' in result:
        raise PercolateAPIError(result, api_url, data, headers)
    return result


def get_all_files(root_directory, with_dotfiles=False, full_path=False,
                  has_file_ext=True):
    all_files = []
    # strip_dir = root_directory.rstrip('/')
    for root, dirs, file_names in os.walk(root_directory):
        if not with_dotfiles:
            file_names = [f for f in file_names if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']
        for f in file_names:
            result = f
            if not has_file_ext:
                result = os.path.splitext(result)[0]
            if full_path:
                result = os.path.join(root, result)
            all_files.append(unicodedata.normalize('NFC', result))
    return all_files


def get_root_folder_id(api_key, license_uid):
    data = {'ids': 'folder:primary', 'scope_ids': license_uid}
    api_url = '/v5/folder/'
    raw_root_details = get_object(api_key, api_url, data)
    return raw_root_details['data'][0]['id']


def _get_license_timezone(api_key, license_uid):
    url = '/v5/license/{}'.format(license_uid)
    license_obj = get_object(api_key, url)['data']
    return license_obj['timezone']


def update_status(api_key, post_obj, status, url=None, live_at='now'):
    post_id = post_obj['id']
    field_list = ['topic_ids', 'term_ids', 'ext', 'description', 'name',
                  'status']
    new_post_obj = {}
    for field in field_list:
        new_post_obj[field] = post_obj[field]

    if live_at == 'now':
        live_at_obj = datetime.utcnow() + timedelta(seconds=5)
    elif live_at is None:
        live_at_obj = None
    elif not isinstance(live_at, datetime):
        live_at_obj = dt_parse(live_at)
    else:
        live_at_obj = live_at

    new_post_obj['live_at'] = live_at_obj.strftime('%Y-%m-%dT%H:%M:%S.000Z') \
        if live_at_obj else None
    if live_at:
        timezone = _get_license_timezone(api_key, post_obj['scope_id'])
        new_post_obj['live_at_timezone'] = timezone
    new_post_obj['status'] = status
    new_post_obj['url'] = url
    api_url = '/v5/post/{}'.format(post_id)

    result = put_object(api_key, api_url, data=new_post_obj)
    return result


    # pprint(new_post_obj)

