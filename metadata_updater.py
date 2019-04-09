import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import argparse
import csv
from copy import deepcopy
from collections import defaultdict, OrderedDict
from pprint import pprint
from dateutil.parser import parse

from ts_utils import (get_all_objects, get_object, get_id_from_uid,
                      put_object, post_object, PercolateAPIError)

from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from google.cloud import bigquery

class MetadataUpdater(object):
    def __init__(self, api_key, license_uid):
        self.api_key = api_key
        self.license_uid = license_uid
        self.license_id = get_id_from_uid(license_uid)
        self.topic_schema_uid = None
        self.schemas_cache = {}
        self.all_custom_schemas = self._get_custom_fields()
        self.taxonomies = self._setup_taxonomies()
        default_metadatas = self._get_system_schema()
        self.asset_schema_uid = default_metadatas[0]['version']['version_id']
        self.usage_schema_uid = default_metadatas[1]['version']['version_id']
        self.tag_cache = {}
        self.name_for_term = {}
        self.full_path_for_term = {}
        self.field_order_list = []
        self.folder_for_id = {}
        self.object_types = ('asset', 'campaign', 'campaign_section', 'task',
                             'channel', 'monitoring_flag', 'post', 'targeting',
                             'post_attachment')

    def _get_system_schema(self):
        s_params = {
            'scope_ids': 'builtin_scope:system',
            'statuses': 'active',
            'type': 'metadata',
            'resource_types': 'asset'
        }
        schemas_call = get_object(self.api_key, '/v5/schema/', params=s_params)
        schemas = schemas_call['data']
        asset_md = [x for x in schemas if x['name'] == 'Asset Metadata'][0]
        usage_md = [x for x in schemas if x['name'] == 'Usage Rights'][0]
        return asset_md, usage_md

    def _get_schema(self, schema_id):
        if schema_id not in self.schemas_cache:
            schema_call = get_object(self.api_key, '/v5/schema/' + schema_id)
            self.schemas_cache[schema_id] = schema_call['data']
        return self.schemas_cache[schema_id]

    def _get_name_path_ids_for_term_id(self, term_uid):
        term_url = '/v5/term/{}'
        try:
            term_call = get_object(self.api_key, term_url.format(term_uid))
            term = term_call['data']
            return term['name'], term['path_ids'][1:]
        except PercolateAPIError:
            return '', []

    def _get_full_path_for_term(self, term_uid):
        path = []
        if term_uid not in self.full_path_for_term:
            # Get term full path from API and name
            (name, path) = self._get_name_path_ids_for_term_id(term_uid)
            self.name_for_term[term_uid] = name
            for n in path:
                if n not in self.name_for_term:
                    self.name_for_term[n] = \
                        self._get_name_path_ids_for_term_id(n)[0]
            path.append(term_uid)
            path_parts = [self.name_for_term.get(x, '') for x in path]
            self.full_path_for_term[term_uid] = '|'.join(path_parts)
        return self.full_path_for_term[term_uid]

    def _get_taxonomies(self, root_ids=None):
        if root_ids is None:
            root_ids = []
        if root_ids:
            params = {'ids': ','.join(root_ids)}
            all_root_terms = get_all_objects(self.api_key, '/v5/term/', params)
            taxonomy_ids = [x['taxonomy_id'] for x in all_root_terms]
            tax_params = {'ids': ','.join(taxonomy_ids)}
        else:
            tax_params = {'scope_ids': self.license_uid}
        # No support of pagination?
        tax_call = get_object(self.api_key, '/v5/taxonomy/', params=tax_params)
        result = tax_call['data']
        return result

    def _prepare_tags(self, tags_list):
        term_ids = []
        for tag in tags_list:
            if tag.startswith('term:'):
                term_ids.append(tag)
                continue
            # cache_stats['count'] += 1
            if tag[:64] in self.tag_cache:
                term_ids.append(self.tag_cache[tag[:64]])
                # cache_stats['cache'] += 1
            else:
                data = {
                    'scope_id': self.license_uid,
                    'namespace': 'tag',
                    'name': tag[:64]
                }
                response = post_object(self.api_key, '/v5/term/', data)
                term_ids.append(response['id'])
                self.tag_cache[tag[:64]] = response['id']
        return term_ids

    def _get_custom_fields(self, flat_list=False, schema_uids=None):
        result = []
        if schema_uids:
            for schema_uid in schema_uids:
                if schema_uid not in self.schemas_cache:
                    schema_obj = get_object(self.api_key, '/v5/schema/' +
                                            schema_uid)['data']
                    self.schemas_cache[schema_uid] = schema_obj
        if self.schemas_cache:
            if schema_uids is None or not isinstance(schema_uids, list):
                schemas = self.schemas_cache.values()
            else:
                schemas = [x for x in self.schemas_cache.values()
                           if x['id'] in schema_uids]
        else:
            s_params = {
                'scope_ids': self.license_uid,
                'statuses': 'active',
                'type': 'metadata',
                # 'resource_types': 'campaign'
            }
            # Schemas call does not support pagination, so get single object
            schemas_call = get_object(self.api_key, '/v5/schema/',
                                      params=s_params)
            schemas = schemas_call['data']
        for schema in schemas:
            if schema['id'] not in self.schemas_cache:
                self.schemas_cache[schema['id']] = schema
            if schema['slug'] == 'topics':
                self.topic_schema_uid = schema['id']
            if flat_list:
                result.extend(schema['fields'])
            else:
                result.append((schema['version']['version_id'],
                               schema['fields']))
        return result

    # Create dictionary for a taxonomy with full path as key, ID as value
    def _create_taxonomy_dict(self, root_uid, tax_depth):
        params = {'mode': 'taxonomy', 'parent_ids': root_uid,
                  'depth': tax_depth}
        all_nodes = get_all_objects(self.api_key, '/v5/term/', params)
        name_for_id = {}
        id_for_path = {}
        for n in all_nodes:
            name_for_id[n['id']] = n['name']
        for n in all_nodes:
            path = n['path_ids'][1:]
            path.append(n['id'])
            path_parts = [name_for_id[x] for x in path]
            full_path = '|'.join(path_parts)
            id_for_path[full_path] = {'leaf': n['id'], 'path': path}
        return id_for_path

    def _setup_taxonomies(self):
        result = {}
        schema_fields_datalist = self._get_custom_fields(flat_list=True)
        term_fields = [x for x in schema_fields_datalist
                       if x['type'] == 'term']
        if not term_fields:
            return result
        all_term_roots = [f['ext']['parent_term_ids'] for f in term_fields]
        term_roots = set([item for sublist in all_term_roots
                          for item in sublist])
        all_taxonomies = self._get_taxonomies(term_roots)
        check_taxes = [x for x in all_taxonomies
                       if x['root_id'] in term_roots]
        for t in check_taxes:
            tree = self._create_taxonomy_dict(t['root_id'], t['max_depth'])
            result[t['root_id']] = tree
            r = list(tree.keys())
            r.sort()
            # print(r)
        return result

    def create_custom_metadata(self, schema_id, metadata_dict,
                               tag_path_terms=False):
        schema_id_list = [schema_id]
        schema_list = self._get_custom_fields(schema_uids=schema_id_list)
        info_for_field = {}
        result = {}
        for s in schema_list:
            for f in s[1]:
                info_for_field[f['label']] = f
                info_for_field[f['label']]['schema_uid'] = s[0]
        for key, value in metadata_dict.items():
            key_info = info_for_field.get(key)
            if not key_info:
                continue
            new_value = value
            # Map select to value
            if key_info['type'] == 'select':
                # all_values = map(str.strip, new_value.split('|'))
                value_for_label = {}
                for x in key_info['ext']['values']:
                    value_for_label[x['label']] = x['value']
                new_value = value_for_label.get(new_value)

            # Map multi-select to value
            if key_info['type'] == 'multi-select':
                # all_values = map(str.strip, new_value.split('|'))
                all_values = new_value.split('|')
                value_for_label = {}
                for x in key_info['ext']['values']:
                    value_for_label[x['label']] = x['value']
                new_value = [value_for_label.get(x) for x in all_values
                             if value_for_label.get(x) is not None]

            # Map string-array to list
            if key_info['type'] == 'string-array':
                all_values = new_value.split('|')
                all_values = [x for x in all_values if x]
                new_value = all_values

            # Map asset to list
            if key_info['type'] == 'asset':
                all_values = new_value.split(',')
                all_values = [x for x in all_values if x.startswith('asset:')]
                new_value = all_values

            # Map date to date format
            if key_info['type'] == 'date':
                try:
                    date_value = parse(new_value)
                    if key_info['ext']['include_time']:
                        time_format = '%Y-%m-%dT%H:%M:%S.000Z'
                        new_value = date_value.strftime(time_format)
                    else:
                        new_value = date_value.date().isoformat()
                except ValueError:
                    new_value = None

            # Validate link
            if key_info['type'] == 'link':
                if not isinstance(new_value, list):
                    all_values = [new_value]
                else:
                    all_values = new_value
                val = URLValidator()
                good_values = []
                for x in all_values:
                    try:
                        val(x)
                        good_values.append(x)
                    except ValidationError:
                        pass
                good_link_objs = []
                for l in good_values:
                    link_params = {'url': l}
                    link_obj = post_object(self.api_key, '/v3/links/',
                                           data=link_params)
                    good_link_objs.append('link:{}'.format(link_obj['id']))
                new_value = good_link_objs

            # Map path to term for taxonomies
            if key_info['type'] == 'term':
                root_id = key_info['ext']['parent_term_ids'][0]
                all_values = new_value.split('||') if new_value else []
                # if not isinstance(new_value, list):
                #     new_value = [new_value]
                if tag_path_terms:
                    new_value = []
                    for x in all_values:
                        if self.taxonomies[root_id].get(x) is not None:
                            new_value.extend(self.taxonomies[root_id].get(x)
                                             ['path'])
                    # new_value = [self.taxonomies[root_id].get(x)['path']
                    #              for x in all_values
                    #              if self.taxonomies[root_id].get(x) is not None]
                else:
                    new_value = [self.taxonomies[root_id].get(x)['leaf']
                                 for x in all_values
                                 if self.taxonomies[root_id].get(x) is not None]
            result[key_info['key']] = new_value
        return result

    def _create_metadata(self, metadata_dict, object_uid, tag_path_terms=False):
        object_type = object_uid.split(':', 1)[0]
        if object_type not in self.object_types:
            print('Object type not recognized')
            return {}
        all_schema_ids = [x['id'] for x in self.schemas_cache.values()
                          if object_type in x['limit_resource_types']]

        all_custom_schemas = self._get_custom_fields(
            schema_uids=all_schema_ids)
        info_for_field = {}
        for s in all_custom_schemas:
            for f in s[1]:
                info_for_field[f['label']] = f
                info_for_field[f['label']]['schema_uid'] = s[0]
        all_new_metadata = defaultdict(dict)
        for key, value in metadata_dict.items():
            key_info = info_for_field.get(key)
            if not key_info:
                continue
            new_value = value

            # Map select to value
            if key_info['type'] == 'select':
                # all_values = map(str.strip, new_value.split('|'))
                value_for_label = {}
                for x in key_info['ext']['values']:
                    value_for_label[x['label']] = x['value']
                new_value = value_for_label.get(new_value)

            # Map multi-select to value
            if key_info['type'] == 'multi-select':
                # all_values = map(str.strip, new_value.split('|'))
                all_values = new_value.split('|')
                value_for_label = {}
                for x in key_info['ext']['values']:
                    value_for_label[x['label']] = x['value']
                new_value = [value_for_label.get(x) for x in all_values
                             if value_for_label.get(x) is not None]

            # Map string-array to list
            if key_info['type'] == 'string-array':
                all_values = new_value.split('|')
                all_values = [x for x in all_values if x]
                new_value = all_values

            # Map path to term for taxonomies
            if key_info['type'] == 'term':
                root_id = key_info['ext']['parent_term_ids'][0]
                all_values = new_value.split('||') if new_value is not None \
                    else []
                # if not isinstance(new_value, list):
                #     new_value = [new_value]
                if tag_path_terms:
                    new_value = []
                    for x in all_values:
                        if self.taxonomies[root_id].get(x) is not None:
                            new_value.extend(self.taxonomies[root_id].get(x)
                                             ['path'])
                    # new_value = [self.taxonomies[root_id].get(x)['path']
                    #              for x in all_values
                    #              if self.taxonomies[root_id].get(x) is not None]
                else:
                    new_value = [self.taxonomies[root_id].get(x)['leaf']
                                 for x in all_values
                                 if self.taxonomies[root_id].get(x) is not None]
            all_new_metadata[key_info['schema_uid']][
                key_info['key']] = new_value
        result = dict(all_new_metadata)
        return result

    def get_custom_metadata(self, object_id, with_schema_name=False, with_raw_terms=False):
        params = {'object_ids': object_id}
        all_md = get_all_objects(self.api_key, '/v5/metadata/', params)
        # pprint(all_md)
        list_order = []
        md_list = []
        all_custom_md = [x for x in all_md if x['schema_id'] not in
                         (self.asset_schema_uid, self.usage_schema_uid)]
        # TODO - use full self.asset_schema_uid/usage_schema_uid
        md_list.extend(all_custom_md)

        result = OrderedDict()
        for md in md_list:
            md_result = self.translate_custom_metadata(
                md['schema_id'], md['ext'], with_schema_name)
            result.update(md_result)

        return dict(result)

    def translate_custom_metadata(self, schema_uid, ext_data,
                                  with_schema_name=False):
        result = OrderedDict()
        list_order = []
        schema = self._get_schema(schema_uid)
        schema_fields = schema['fields']
        schema_name = schema['name']
        for field in schema_fields:
            value = ext_data.get(field['key'])
            final_value = value
            # Translate
            if field['type'] == 'multi-select':
                final_value = '|'.join(value) if value else []
            elif field['type'] in ('term', 'term_id'):
                if not value:
                    final_value = []
                all_values = [self._get_full_path_for_term(x)
                              for x in final_value]

                if field['type'] == 'term':
                    final_value = '||'.join(all_values)
                else:
                    final_value = ','.join(all_values)
            final_value = None if not final_value else final_value
            if schema["name"] == "Usage Rights":  # Shouldn't happen
                label_for_field = {
                    'expiration': 'Expiration Date',
                    'title': 'License Type',
                    'description': 'Usage Rights Information'
                }
                field_label = label_for_field[field['key']]
            elif with_schema_name:
                field_label = '{}: {}'.format(schema_name, field['label'])
            else:
                field_label = field['label']
            result[field_label] = final_value
            for f in list_order:
                if f not in result:
                    result[f] = None
            list_order.append(field_label)
            if not self.field_order_list or len(self.field_order_list) \
                    < len(list_order):
                self.field_order_list = list_order
        return dict(result)

    def download_taxonomies(self):
        result = {}
        all_data = []
        for root_term, values in self.taxonomies.items():
            root_term_obj = get_object(self.api_key,
                                       '/v5/term/{}'.format(root_term))
            taxonomy_id = root_term_obj['data']['taxonomy_id']
            taxonomy_obj = get_object(self.api_key,
                                      '/v5/taxonomy/{}'.format(taxonomy_id))
            taxonomy_name = taxonomy_obj['data']['name']
            result[taxonomy_name] = sorted(list(values.keys()))
        for name, term_list in result.items():
            print(name)
            for x in term_list:
                print('\t{}'.format(x))
            print('\n')
        # pprint(result)

    def update_custom_metadata(self, object_uid, metadata_dict,
                               tag_path_terms=False):
        new_metadata = self._create_metadata(metadata_dict, object_uid,
                                             tag_path_terms)

        # New method
        existing_metadata = get_object(self.api_key, '/v5/metadata/',
                                       {'object_ids': object_uid})['data']
        custom_metadata = [x for x in existing_metadata if x['schema_id'] not
                           in (self.asset_schema_uid, self.usage_schema_uid)]
        payloads = {}
        metadata_id_for_schema = {}
        for x in custom_metadata:
            payloads[x['schema_id']] = x['ext']
            metadata_id_for_schema[x['schema_id']] = x['id']
        for schema_id, fields_dict in new_metadata.items():
            if schema_id not in payloads:
                payloads[schema_id] = {}
                # Schema not in existing custom metadata, but it can be created.
                # payload['schemas'].append({'schema_uid': schema_id, 'data': {}})
                # s_index = len(payload['schemas']) - 1
            for key, new_value in fields_dict.items():
                payloads[schema_id][key] = new_value
        for schema_id, ext in payloads.items():
            new_payload = {
                'schema_id': schema_id,
                'object_id': object_uid,
                'ext': ext
            }

            if schema_id in metadata_id_for_schema:
                if schema_id not in new_metadata:
                    continue
                m_id = metadata_id_for_schema[schema_id]
                put_object(self.api_key, '/v5/metadata/{}'.format(m_id),
                           new_payload)
            else:
                post_object(self.api_key, '/v5/metadata/', new_payload)

    def get_all_possible_metadata_values(self):
        info_for_field = {}
        all_values_for = {}
        for s in self.all_custom_schemas:
            for f in s[1]:
                info_for_field[f['label']] = f
                info_for_field[f['label']]['schema_uid'] = s[0]
        root_for_field = {k: v['ext']['parent_term_ids'][0]
                          for k, v in info_for_field.items()
                          if v['type'] == 'term'}
        for label, root_id in root_for_field.items():
            the_keys = list(self.taxonomies[root_id].keys())
            all_terms = [x for x in the_keys
                         if self.taxonomies[root_id][x]['leaf'] != root_id]
            all_values = '||'.join(all_terms)
            all_values_for[label] = all_values
        select_values = {k: v['ext']['values']
                         for k, v in info_for_field.items()
                         if v['type'] in ('select', 'multi-select')}
        for label, all_selects in select_values.items():
            all_values_for[label] = '||'.join(all_selects)
        return all_values_for

    def copy_metadata(self, schema_uid, source_object_uid, target_object_uid):
        source_params = {'object_ids': source_object_uid,
                         'schema_id': schema_uid}
        check_params = {'object_ids': target_object_uid,
                        'schema_id': schema_uid}
        url = '/v5/metadata/'
        source_metadata_resp = get_object(self.api_key, url, source_params)
        if source_metadata_resp['meta']['total'] == 0:
            print('No metadata for schema {} on object {}'
                  .format(schema_uid, source_object_uid))
            return None
        source_metadata = source_metadata_resp['data'][0]
        md_obj_resp = get_object(self.api_key, url, check_params)
        md_obj_exists = md_obj_resp['meta']['total']
        for field in ('created_at', 'updated_at', 'id'):
            del source_metadata[field]
        source_metadata['object_id'] = target_object_uid
        if md_obj_exists:
            md_obj_id = md_obj_resp['data'][0]['id']
            put_object(self.api_key, url + md_obj_id,
                       source_metadata)
            result = md_obj_id
        else:
            response = post_object(self.api_key, url, source_metadata)
            result = response['id']
        return result

    def merge_metadata(self, schema_uid, source_object_uid, target_object_uid,
                       dry_run=False):
        url = '/v5/metadata/'
        source_params = {'object_ids': source_object_uid,
                         'schema_id': schema_uid}
        source_obj_resp = get_object(self.api_key, url, source_params)
        if not source_obj_resp['meta']['total']:
            print('No source metadata for {} {}'.format(
                schema_uid, source_object_uid))
            return
        source_metadata = source_obj_resp['data'][0]

        check_params = {'object_ids': target_object_uid,
                        'schema_id': schema_uid}
        md_obj_resp = get_object(self.api_key, url, check_params)
        md_obj_exists = md_obj_resp['meta']['total']
        for field in ('created_at', 'updated_at', 'id'):
            del source_metadata[field]
        source_metadata['object_id'] = target_object_uid
        if not md_obj_exists:
            if dry_run:
                print('POST {} to {}'.format(source_object_uid,
                                             target_object_uid))
                pprint(source_metadata)
            else:
                post_object(self.api_key, url, source_metadata)
        else:
            existing_md = md_obj_resp['data'][0]['ext']
            adding_md = source_metadata['ext']
            schema = self._get_schema(schema_uid)
            new_md = {}
            for field in schema['fields']:
                if field['type'] == 'term' and field['ext']['limit'] is None:
                    key = field['key']
                    # TODO - associate to empty value of schema field dynamically
                    existing = existing_md.get(key, [])
                    adding = adding_md.get(key, [])
                    new_md[key] = list(set(existing) | set(adding))
            new_md_obj = {'object_id': target_object_uid,
                          'schema_id': schema_uid,
                          'ext': new_md}
            md_obj_id = md_obj_resp['data'][0]['id']
            if dry_run:
                print('PUT {} to {} ({})'.format(source_object_uid,
                                                 target_object_uid, md_obj_id))
                pprint(new_md_obj)
            else:
                put_object(self.api_key, url + md_obj_id, new_md_obj)

    def update_metadata(self, object_uid, metadata_dict):
        existing_metadata = get_object(self.api_key, '/v5/metadata/',
                                       {'object_ids': object_uid})['data']
        asset_metadata_id = next((x['id'] for x in existing_metadata
                                  if x['schema_id'] == self.asset_schema_uid),
                                 None)
        usage_metadata_id = next((x['id'] for x in existing_metadata
                                  if x['schema_id'] == self.usage_schema_uid),
                                 None)

        asset_metadata = {
            'schema_id': self.asset_schema_uid,
            'object_id': object_uid,
            'ext': {
                'title': metadata_dict['Title'],
                'description': metadata_dict['Description'],
            }
        }
        if metadata_dict['Tags']:
            tags = self._prepare_tags([tag.strip() for tag in
                                       metadata_dict['Tags'].split(',')])
            asset_metadata['ext']['tags'] = tags
        else:
            asset_metadata['ext']['tags'] = []

        if asset_metadata_id:
            md_url = '/v5/metadata/{}'.format(asset_metadata_id)
            put_object(self.api_key, md_url, asset_metadata)
        else:
            md_url = '/v5/metadata/'
            post_object(self.api_key, md_url, asset_metadata)

        if metadata_dict['Add Usage Rights Information'] and \
                metadata_dict['Add Usage Rights Information'] not in\
                ('No', 'no', 'NO', '0'):
            usage_metadata = {
                'schema_id': self.usage_schema_uid,
                'object_id': object_uid,
            }
            uri = {}
            translation = {
                'License Type': 'title',
                'Usage Rights Information': 'description',
                'Expiration Date': 'expiration'
            }
            for f in translation.keys():
                uri[translation[f]] = metadata_dict[f]
            usage_metadata['ext'] = uri

            if usage_metadata_id:
                md_url = '/v5/metadata/{}'.format(usage_metadata_id)
                put_object(self.api_key, md_url, usage_metadata)
            else:
                md_url = '/v5/metadata/'
                post_object(self.api_key, md_url, usage_metadata)

    def update(self, object_uid, metadata_dict, standard=True, custom=True,
               tag_path_terms=False):
        if not (standard or custom):
            raise ValueError('Must check standard and/or custom metadata.')
        try:
            if standard and object_uid.startswith('asset:'):
                self.update_metadata(object_uid, metadata_dict)
            if custom:
                self.update_custom_metadata(object_uid, metadata_dict,
                                            tag_path_terms=tag_path_terms)
            return True
        except Exception as e:
            e_type = type(e).__name__
            print('{}: {}'.format(e_type, e))
            return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', required=True)
    parser.add_argument('--metadata')
    parser.add_argument('--key-field', required=False)
    parser.add_argument('--license-uid')

    args = parser.parse_args()
    api_key = args.api_key
    asset_license_uid = args.license_uid
    asset_uid = 'asset:877206581386725093'
    metadata = {
        'Title': 'Asset Title',
        'Description': 'Asset Description',
        'Tags': 'Tag, Another Tag, 3rd Tag',
        'Add Usage Rights Information': '1',
        'License Type': 'Creative Commons',
        'Usage Rights Information': 'For Use on YouTube',
        'Expiration Date': '2019-12-31',
        'Custom Metadata Field': "United States|Maine|Portland"
    }
    cm = MetadataUpdater(api_key, asset_license_uid)
    cm.update(asset_uid, metadata, custom=True)
