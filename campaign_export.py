from collections import OrderedDict
import os
import csv
import argparse
import operator
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from ts_utils import get_all_objects, get_object, PercolateAPIError
from metadata_updater import MetadataUpdater


class CSVCampaignExport(object):
    def __init__(self, api_key):
        self.api_key = api_key
        self.field_type = OrderedDict([
            ('id', 'text'), ('title', 'text'), ('description', 'text'),
            ('term_ids', 'terms'), ('topic_ids', 'topics'),
            ('scope_id', 'text'), ('start_at', 'date'), ('end_at', 'date'),
            ('platform_ids', 'platforms'), ('budget', 'budget'),
            ('thumbnail_asset_id', 'text'), ('created_at', 'date'),
            ('updated_at', 'date'), ('parent_id', 'text')
        ])
        self.header_for = OrderedDict([
            ('id', 'Campaign ID'), ('title', 'Title'),
            ('description', 'Description'), ('term_ids', 'Terms'),
            ('topic_ids', 'Topics'), ('scope_id', 'License ID'),
            ('start_at', 'Start At'), ('end_at', 'End At'),
            ('platform_ids', 'Platforms'), ('budget', 'Budget'),
            ('thumbnail_asset_id', 'Thumbnail Asset'),
            ('created_at', 'Created At'), ('updated_at', 'Updated At'),
            ('parent_id', 'Parent Campaign ID')
        ])
        self.header = ['Title', 'Description', 'License ID',
                       'Start At', 'End At', 'Platforms',
                       'Budget', 'Campaign ID', 'Parent Campaign ID',
                       'Terms', 'Topics',
                       'Thumbnail Asset', 'Created At', 'Updated At'
                       ]
        self.format_function = {'text': self._format_text,
                                'terms': self._format_terms,
                                'topics': self._format_topics,
                                'date': self._format_date,
                                'platforms': self._format_platforms,
                                'budget': self._format_budget
                                }
        self.name_cache = defaultdict(dict)
        self.separator = '|'

    def _format_text(self, input_str, campaign_uid):
        return input_str

    def _format_terms(self, input_list, campaign_uid):
        return self._get_object_names(input_list, '/v5/term/')

    def _format_platforms(self, input_list, campaign_uid):
        return self._get_object_names(input_list, '/v5/platform/')

    def _format_topics(self, input_list, campaign_uid):
        if not self.topic_schema:
            return None
        params = {'object_ids': campaign_uid, 'schema_id': self.topic_schema}
        all_md_objects = get_all_objects(self.api_key, '/v5/metadata/', params)
        if not all_md_objects:
            return None
        topic_term_list = all_md_objects[0]['ext']['topics']
        return self._format_terms(topic_term_list, campaign_uid)

    def _format_date(self, input_str, campaign_uid):
        if input_str is None:
            return None
        dt = parse(input_str)
        return dt.isoformat()

    def _format_budget(self, input_obj, campaign_uid):
        if input_obj is None:
            return None
        if input_obj['currency'] == 'USD':
            return '${0:.2f}'.format(float(input_obj['amount']))
        else:
            return '{} {}'.format(input_obj['amount'], input_obj['currency'])

    def _get_topic_schema(self, license_uid):
        params = {'resource_types': 'campaign', 'scope_ids': license_uid,
                  'type': 'metadata'}
        all_schemas = get_object(self.api_key, '/v5/schema/', params)['data']
        topics_schemas = [x for x in all_schemas if x['slug'] == 'topics']
        if topics_schemas:
            self.topic_schema = topics_schemas[0]['id']
        else:
            self.topic_schema = None

    def _get_object_names(self, object_ids, object_url):
        if not object_ids:
            return None
        translation = OrderedDict((x, None) for x in object_ids)
        object_type = object_ids[0].split(':', 1)[0]
        lookup_items = []
        for item in translation:
            if item in self.name_cache[object_type]:
                translation[item] = self.name_cache[object_type][item]
            else:
                lookup_items.append(item)
        if lookup_items:
            term_ids_str = ','.join(lookup_items)
            params = {'ids': term_ids_str}
            all_terms_objs = get_all_objects(self.api_key, object_url,
                                             params=params)
            name_for_id = {}
            for x in all_terms_objs:
                name_for_id[x['id']] = x['name']
                translation[x['id']] = x['name']
            for id, name in name_for_id.items():
                self.name_cache[object_type][id] = name

        result = list(map(lambda x: translation[x], translation.keys()))
        return self.separator.join(result)

    def get_export(self, license_uid, out_dir=None, params_dict=None,
                   since=None, extend_scopes=False):
        self._get_topic_schema(license_uid)
        mu = MetadataUpdater(self.api_key, license_uid)
        params = {'scope_ids': license_uid}
        if extend_scopes:
            params['extend_scopes'] = True
        if params_dict and isinstance(params_dict, dict):
            for k, v in params_dict.items():
                params[k] = v
        all_campaigns = get_all_objects(self.api_key, '/v5/campaign/',
                                        params=params)
        if since is not None:
            if isinstance(since, datetime):
                n_days_ago = since
            elif isinstance(since, int):
                n_days_ago = datetime.now(timezone.utc) \
                             - timedelta(days=since)
            else:  # since is an integer
                try:
                    n_days_ago = parse(since)
                except (TypeError, ValueError):
                    # Not sure this is the best option
                    n_days_ago = datetime.now(timezone.utc)
            all_campaigns = [x for x in all_campaigns if
                         parse(x['updated_at']) > n_days_ago]
        print('{} campaigns to export'.format(len(all_campaigns)))
        all_data_rows = []
        all_data_dicts = []
        all_data_headers = self.header.copy()
        for campaign in all_campaigns:
            campaign_uid = campaign['id']
            data_row = []
            data_dict = {}
            for field_name, format_key in self.field_type.items():

                func = self.format_function.get(format_key)
                # Route each field through its proper processor
                if func is None:
                    pass
                else:
                    data_row.append(func(campaign[field_name], campaign_uid))
                    data_dict[self.header_for[field_name]] = \
                        func(campaign[field_name], campaign_uid)

            # Add metadata
            camp_md = mu.get_custom_metadata(campaign_uid,
                                             with_schema_name=True,
                                             with_raw_terms=True)
            camp_md.pop('Topics: Topics', None)
            data_dict.update(camp_md)
            for md_label in camp_md.keys():
                if md_label not in all_data_headers:
                    all_data_headers.append(md_label)

            all_data_rows.append(data_row)
            all_data_dicts.append(data_dict)
            if len(all_data_rows) % 100 == 0:
                print('\t{} campaigns completed'.format(len(all_data_rows)))

        all_data_rows.sort(key=operator.itemgetter(0))
        all_data_dicts.sort(key=lambda x: x[self.header_for['id']])

        campaign_data = []
        campaign_data.append(all_data_headers)
        for x in all_data_dicts:
            campaign_data.append([x.get(y) for y in all_data_headers])

        if out_dir:

            file_name = license_uid.replace(':', '_') + '_campaign_export.csv'
            file_path = os.path.join(out_dir, file_name)
            with open(file_path, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=all_data_headers,
                                        extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_data_dicts)
            return file_path
        else:
            return all_data_headers, all_data_dicts


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', required=True)
    parser.add_argument('--license-uid', required=True, nargs='+', type=str)
    parser.add_argument('--out-directory', default=os.path.curdir)
    parser.add_argument('--extend-scopes', action='store_true')
    args = parser.parse_args()
    params_dict = None

    if args.extend_scopes:
        extend_scopes = True
    else:
        extend_scopes = False

    ce = CSVCampaignExport(args.api_key)
    for license_uid in args.license_uid:
        print(license_uid)
        all_data = ce.get_export(license_uid, args.out_directory, params_dict,
                                 extend_scopes=extend_scopes
                                 )
