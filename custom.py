import html
import math

from datetime import  datetime

from cartmigration.models.basecart import LeBasecart
from cartmigration.libs.utils import *


class LeCartCustom(LeBasecart):
	def display_config_source(self):
		parent = super().display_config_source()
		if parent['result'] != 'success':
			return parent
		response = response_success()
		order_status_data = {
			'completed': 'Completed'
		}
		language_data = {
			1: "Default Language"
		}
		self._notice['src']['category_root'] = 1
		self._notice['src']['site'] = {
			1: 'Default Shop'
		}
		self._notice['src']['category_data'] = {
			1: 'Default Category',
		}
		self._notice['src']['support']['language_map'] = True
		self._notice['src']['support']['country_map'] = False
		self._notice['src']['support']['customer_group_map'] = False
		self._notice['src']['support']['taxes'] = True
		self._notice['src']['support']['manufacturers'] = False
		self._notice['src']['support']['reviews'] = False
		self._notice['src']['support']['add_new'] = True
		self._notice['src']['support']['skip_demo'] = False
		self._notice['src']['support']['customer_group_map'] = False
		self._notice['src']['languages'] = language_data
		self._notice['src']['order_status'] = order_status_data
		response['result'] = 'success'
		return response

	def display_config_target(self):
		return response_success()

	def display_import_source(self):
		if self._notice['config']['add_new']:
			recent = self.get_recent(self._migration_id)
			if recent:
				types = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders', 'reviews', 'pages', 'blogs', 'coupons', 'cartrules']
				for _type in types:
					self._notice['process'][_type]['id_src'] = recent['process'][_type]['id_src']
					self._notice['process'][_type]['total'] = 0
					self._notice['process'][_type]['imported'] = 0
					self._notice['process'][_type]['error'] = 0
		queries = {
			# 'taxes': {
			# 	'type': 'select',
			# 	'query': "SELECT COUNT(1) AS count FROM  _DBPRF_StateTaxStates WHERE id > " + to_str(self._notice['process']['taxes']['id_src']),
			# },
			# 'manufacturers': {
			# 	'type': 'select',
			# 	'query': "SELECT COUNT(1) AS count  FROM _DBPRF_manufacturers WHERE manufacturers_id > " + to_str(self._notice['process']['manufacturers']['id_src']),
			# },
			'categories': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM  _DBPRF_categories WHERE categories_id > " + to_str(self._notice['process']['categories']['id_src']),
			},
			'products': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM  _DBPRF_products WHERE products_id > " + to_str(self._notice['process']['products']['id_src']),
			},
			'customers': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM  _DBPRF_customers WHERE customers_id > " + to_str(self._notice['process']['customers']['id_src']),
			},
			'orders': {
				'type': 'select',
				'query': "SELECT COUNT(1) AS count FROM  _DBPRF_orders WHERE  orders_id > " + to_str(self._notice['process']['orders']['id_src']),
			},
			# 'reviews': {
			# 	'type': 'select',
			# 	'query': "SELECT COUNT(1) AS count FROM _DBPRF_reviews WHERE reviews_id > " + to_str(self._notice['process']['reviews']['id_src']),
			# },
		}
		count = self.select_multiple_data_connector(queries)
		if (not count) or (count['result'] != 'success'):
			return response_error()
		real_totals = dict()
		for key, row in count['data'].items():
			total = self.list_to_count_import(row, 'count')
			real_totals[key] = total
		for key, total in real_totals.items():
			self._notice['process'][key]['total'] = total
		return response_success()

	def display_import_target(self):
		return response_success()

	def display_confirm_source(self):
		return response_success()

	def display_confirm_target(self):
		self._notice['target']['clear']['function'] = 'clear_target_taxes'
		return response_success()
# display
# ---------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
	# TODO: CLEAR

	def clear_target_taxes(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_manufacturers',
		}
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_manufacturers(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_categories',
		}
		self._notice['target']['clear'] = next_clear
		return next_clear

	def clear_target_categories(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_products',
		}

		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	def clear_target_products(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_customers',
		}

		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	def clear_target_customers(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_orders',
		}

		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	def clear_target_orders(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_reviews',
		}

		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	def clear_target_reviews(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_pages',
		}

		self._notice['target']['clear'] = next_clear
		return self._notice['target']['clear']

	# TODO: TAX
	def prepare_taxes_import(self):
		return self

	def prepare_taxes_export(self):
		return self

	def get_taxes_main_export(self):
		id_src = self._notice['process']['taxes']['id_src']
		limit = self._notice['setting']['taxes']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_StateTaxStates WHERE id > " + to_str(
				id_src) + " ORDER BY id ASC LIMIT " + to_str(limit)
		}
		taxes = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not taxes or taxes['result'] != 'success':
			return response_error('could not get taxes main to export')
		return taxes

	def get_taxes_ext_export(self, taxes):
		return response_success()

	def convert_tax_export(self, tax, taxes_ext):
		tax_product = list()
		tax_customer = list()
		tax_zone = list()
		tax_product_data = self.construct_tax_product()
		tax_product_data['id'] = 1
		tax_product_data['code'] = None
		tax_product_data['name'] = 'Product Tax Class Shopify'
		tax_product.append(tax_product_data)

		tax_zone_state = self.construct_tax_zone_state()

		tax_zone_country = self.construct_tax_zone_country()
		tax_zone_country['id'] = 'US'
		tax_zone_country['name'] = 'United States'
		tax_zone_country['country_code'] = 'US'

		tax_zone_rate = self.construct_tax_zone_rate()
		tax_zone_rate['id'] = None
		tax_zone_rate['name'] = tax['state']+' '+ tax['rate']
		tax_zone_rate['rate'] = tax['rate']

		tax_zone_data = self.construct_tax_zone()
		tax_zone_data['id'] = None
		tax_zone_data['name'] = 'United States'
		tax_zone_data['country'] = tax_zone_country
		tax_zone_state = self.construct_tax_zone_state()
		tax_zone_state['id'] = 'TX'
		tax_zone_state['name'] = 'Texas'
		tax_zone_state['state_code'] = 'TX'


		tax_zone_data['state'] = tax_zone_state
		tax_zone_data['rate'] = tax_zone_rate
		tax_zone.append(tax_zone_data)

		tax_data = self.construct_tax()
		tax_data['id'] = tax['id']
		tax_data['name'] = tax['state']+' '+tax['rate']
		tax_data['tax_products'] = tax_product
		tax_data['tax_zones'] = tax_zone
		return response_success(tax_data)
# -----------------------------------------------------------------------
	def get_tax_id_import(self, convert, tax, taxes_ext):
		return tax['id']

	def check_tax_import(self, convert, tax, taxes_ext):
		return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id']) else False

	def router_tax_import(self, convert, tax, taxes_ext):
		return response_success('tax_import')

	def before_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	def tax_import(self, convert, tax, taxes_ext):
		return response_success(0)

	def after_tax_import(self, tax_id, convert, tax, taxes_ext):
		return response_success()

	def addition_tax_import(self, convert, tax, taxes_ext):
		return response_success()

	# TODO: MANUFACTURER
	def prepare_manufacturers_import(self):
		return self

	def prepare_manufacturers_export(self):
		return self
# ------------------------------------------------------------
	def get_manufacturers_main_export(self):
		id_src = self._notice['process']['manufacturers']['id_src']
		limit = self._notice['setting']['manufacturers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_manufacturers WHERE manufacturers_id > " + to_str(
				id_src) + " ORDER BY manufacturers_id ASC LIMIT " + to_str(limit),
		}
		manufacturers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not manufacturers or manufacturers['result'] != 'success':
			return response_error('could not get manufacturers main to export')
		return manufacturers

	def get_manufacturers_ext_export(self, manufacturers):
		url_query = self.get_connector_url('query')
		manufacturer_ids = duplicate_field_value_from_list(manufacturers['data'], 'manufacturer_id')
		manufacturers_ext_queries = {
			'manufacturers_info': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_manufacturers_info WHERE manufacturers_id IN " + self.list_to_in_condition(
					manufacturer_ids)
			}
		}
		manufacturers_ext = self.get_connector_data(url_query,
		                                            {'serialize': True, 'query': json.dumps(manufacturers_ext_queries)})
		if not manufacturers_ext or manufacturers_ext['result'] != 'success':
			return response_error()
		return manufacturers_ext

	def convert_manufacturer_export(self, manufacturer, manufacturers_ext):
		manufacturer_data = self.construct_manufacturer()
		manufacturer_data['id'] = manufacturer['manufacturers_id']
		manufacturer_data['name'] = manufacturer['manufacturers_name']
		manufacturer_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['image_manufacturer'])
		manufacturer_data['thumb_image']['path'] = manufacturer['manufacturers_image']

		for language_id, language_label in self._notice['src']['languages'].items():
			manufacturer_language_data = dict()
			manufacturer_language_data['name'] = manufacturer['manufacturers_name']
			manufacturer_data['languages'][language_id] = manufacturer_language_data
		return response_success(manufacturer_data)

	def get_manufacturer_id_import(self, convert, manufacturer, manufacturers_ext):
		return manufacturer['manufacturers_id']

	def check_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return True if self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['id']) else False

	def router_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success('manufacturer_import')

	def before_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()

	def manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success(0)

	def after_manufacturer_import(self, manufacturer_id, convert, manufacturer, manufacturers_ext):
		return response_success()

	def addition_manufacturer_import(self, convert, manufacturer, manufacturers_ext):
		return response_success()
# category
# 	TODO: CATEGORY
	def prepare_categories_import(self):
		return self

	def prepare_categories_export(self):
		return self
	def get_categories_main_export(self):
		id_src = self._notice['process']['categories']['id_src']
		limit = self._notice['setting']['categories']
		query = {
			'type': 'select',
			'query': "SELECT * FROM  _DBPRF_categories WHERE categories_id > " + to_str(
				id_src) + " ORDER BY categories_id ASC LIMIT " + to_str(limit)
		}
		categories = self.select_data_connector(query)
		if not categories or categories['result'] != 'success':
			return response_error('could not get manufacturers main to export')
		return categories
	def get_categories_ext_export(self, categories):
		url_query = self.get_connector_url('query')
		category_ids = duplicate_field_value_from_list(categories['data'], 'categories_id')
		categories_ext_queries = {
			'categories_description': {
				'type': 'select',
				'query': "SELECT * FROM  _DBPRF_categories_description WHERE categories_id IN " + self.list_to_in_condition(
					category_ids)
			},
		}
		categories_ext = self.select_multiple_data_connector(categories_ext_queries)

		if not categories_ext or categories_ext['result'] != 'success':
			return response_warning()
		return categories_ext

	def convert_category_export(self, category, categories_ext):
		category_data = self.construct_category()
		parent = self.construct_category_parent()
		parent['id'] = category['parent_id']
		if category['parent_id']:
			parent_data = self.get_category_parent(category['parent_id'])
			if parent_data['result'] == 'success':
				parent = parent_data['data']
		category_data['id'] = category['categories_id']
		category_data['parent'] = parent
		category_data['active'] = True
		# if self.image_exist(self.get_url_suffix(self._notice['src']['config']['categories_image']), 'c/' + to_str(category['categories_id']) + '.jpg'):
		# 	category_data['thumb_image']['url'] = self.get_url_suffix(self._notice['src']['config']['categories_image'])
		# 	category_data['thumb_image']['path'] = 'c/' + to_str(category['categories_id']) + '.jpg'
		# 	category_data['thumb_image']['label'] = category['categories_id']
		category_data['thumb_image']['path'] = category['categories_image']
		category_data['sort_order'] = category['sort_order']
		category_data['created_at'] = category['date_added']
		category_data['updated_at'] = category['last_modified']
		category_description = get_row_from_list_by_field(categories_ext['data']['categories_description'], 'categories_id', category['categories_id'])
		category_data['meta_description'] = ''
		if category_description:
			category_data['description'] = category_description['categories_name'] if category_description['categories_name'] != '' else category_description['categories_id']
		else:
			category_data['description'] = ''
		category_data['name'] = category_description['categories_name']
		category_data['meta_title'] = ''
		category_data['meta_keyword'] = ''
		category_data['category'] = category
		category_data['categories_ext'] = categories_ext
		if self._notice['config']['seo_301']:
			detect_seo = self.detect_seo()
			category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)
		return response_success(category_data)

	def get_category_id_import(self, convert, category, categories_ext):
		return category['categories_id']

	def check_category_import(self, convert, category, categories_ext):
		id_imported = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code'])
		return id_imported

	def router_category_import(self, convert, category, categories_ext):
		return response_success('category_import')

	def before_category_import(self, convert, category, categories_ext):
		return response_success()

	def category_import(self, convert, category, categories_ext):
		return response_success(0)

	def after_category_import(self, category_id, convert, category, categories_ext):
		return response_success()

	def addition_category_import(self, convert, category, categories_ext):
		return response_success()

	# TODO: PRODUCT
	def prepare_products_import(self):
		return self
	def prepare_products_export(self):
		return self
	def get_products_main_export(self):
		id_src = self._notice['process']['products']['id_src']
		limit = self._notice['setting']['products']
		query = {
			'type': 'select',
			'query': "SELECT * FROM  _DBPRF_products WHERE products_id > " + to_str(id_src) + " ORDER BY products_id ASC LIMIT " + to_str(limit)
		}

		# products = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		products = self.select_data_connector(query, 'products')
		if not products or products['result'] != 'success':
			return response_error()
		return products
	def get_products_ext_export(self, products):
		url_query = self.get_connector_url('query')
		product_ids = duplicate_field_value_from_list(products['data'], 'products_id')
		product_id_con = self.list_to_in_condition(product_ids)
		product_id_query = self.product_to_in_condition_seourl(product_ids)

		product_ext_queries = {
			'products_description': {
				'type': "select",
				'query': "SELECT * FROM products_description WHERE products_id IN " + product_id_con,
			},
			'products_to_categories': {
				'type': 'select',
				'query': "SELECT * FROM products_to_categories WHERE products_id IN " + product_id_con,
			},
			'product_attribute': {
				'type': 'select',
				'query': "SELECT * FROM product_attribute WHERE product_id IN " + product_id_con,
			},
			'attribute': {
				'type': 'select',
				'query': "SELECT * FROM attribute as attr INNER JOIN product_attribute as pa" + " ON attr.attribute_id = pa.attribute_id WHERE pa.product_id IN " + product_id_con,
			},
		}

		product_ext = self.select_multiple_data_connector(product_ext_queries, "products")
		if (not product_ext) or product_ext['result'] != 'success':
			return response_error()

		product_ext = self.select_multiple_data_connector(product_ext_queries, "products")
		if (not product_ext) or product_ext['result'] != 'success':
			return response_error()

		product_ext_rel_queries = {
		}

		product_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, "products_description")
		if (not product_ext_rel) or (product_ext_rel['result'] != 'success'):
			return response_error()

		product_ext = self.sync_connector_object(product_ext, product_ext_rel)
		return product_ext

	# url_query = self.get_connector_url('query')
		# product_ids = duplicate_field_value_from_list(products['data'], 'products_id')
		# product_id_con = self.list_to_in_condition(product_ids)
		# product_id_query = self.product_to_in_condition_seourl(product_ids)
		#
		# product_ext_queries = {
		# 	'products_description': {
		# 		'type': "select",
		# 		'query': "SELECT * FROM products_description WHERE products_id IN " + product_id_con,
		# 	},
		# 	'products_to_categories': {
		# 		'type': 'select',
		# 		'query': "SELECT * FROM products_to_categories WHERE products_id IN " + product_id_con,
		# 	},
		# 	'product_attribute': {
		# 		'type': 'select',
		# 		'query': "SELECT * FROM product_attribute WHERE product_id IN " + product_id_con,
		# 	},
		# 	'attribute': {
		# 		'type': 'select',
		# 		'query': "SELECT * FROM attribute as att INNER JOIN product_attribute as pr_att" + " ON att.attribute_id = pr_att.attribute_id WHERE pa.product_id IN  " + product_id_con,
		# 	},
		#
		# }
		#
		# product_ext = self.select_multiple_data_connector(product_ext_queries, "products")
		# if (not product_ext) or product_ext['result'] != 'success':
		# 	return response_error()
		#
		# product_ext_rel_queries = {
		# }
		#
		# product_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, "products_description")
		# if (not product_ext_rel) or (product_ext_rel['result'] != 'success'):
		# 	return response_error()
		#
		# product_ext = self.sync_connector_object(product_ext, product_ext_rel)
		# return product_ext

	def convert_product_export(self, product, products_ext):
		self.log(products_ext, 'products_ext')
		self.log(product, 'product')
		products_ext_data = products_ext['data']
		product_data = self.construct_product()
		product_data['id'] = product['products_id']
		product_data['sku'] = product['products_upc_code']
		product_data['price'] = product['products_price']
		product_data['weight'] = product['products_weight']
		if to_int(product['products_status']) > 0:
			status = True
		else:
			status = False
		product_data['status'] = product['products_status']
		product_data['manage_stock'] = True
		product_data['qty'] = product['products_quantity']
		product_data['length'] = to_decimal(product['products_length'])
		product_data['date_available'] = product['products_date_available']
		product_data['created_at'] = to_str(datetime.fromtimestamp(to_int(product['products_date_added']))) if product['products_date_added'] else get_current_time()
		product_data['updated_at'] = to_str(datetime.fromtimestamp(to_int(product['products_last_modified']))) if to_int(product['products_last_modified']) else product_data['created_at']
		product_description = get_list_from_list_by_field(products_ext_data['products_description'], 'products_id', product['products_id'])[0]
		product_data['name'] = product_description['products_name']
		product_data['description'] = html.unescape(product_description['products_description'])

		url_product_image = 'http://localhost/customcart/images/'
		check_thumbnail = False
		if product['products_image'] != '':
			check_thumbnail = True
			product_data['thumb_image']['url'] = url_product_image
			product_data['thumb_image']['path'] = product['products_image']


		product_categories = get_list_from_list_by_field(products_ext_data['products_to_categories'], 'products_id', product['products_id'])
		if product_categories:
			for product_category in product_categories:
				product_category_data = self.construct_product_category()
				product_category_data['id'] = product_category['categories_id']
				product_data['categories'].append(product_category_data)
		# convert attributes
		product_attributes = get_list_from_list_by_field(products_ext_data['attribute'], 'product_id', product['products_id'])
		if product_attributes:
			for product_attribute in product_attributes:
				product_attribute_data = self.construct_product_attribute()
				product_attribute_data['option_id'] = product_attribute['attribute_code']
				product_attribute_data['option_code_save'] = product_attribute['attribute_code']
				product_attribute_data['option_code'] = product_attribute['attribute_code']
				product_attribute_data['option_type'] = product_attribute['attribute_type']
				product_attribute_data['option_name'] = product_attribute['attribute_name']
				product_attribute_data['option_value_name'] = product_attribute['value']
				product_data['attributes'].append(product_attribute_data)
		a = 1
		return response_success(product_data)

	def get_product_id_import(self, convert, product, products_ext):
		return product['products_id']
	def check_import(self, convert, product, products_ext):
		id_imported= self.get_map_field_by_src(self.TYPE_PRODUCT, convert['products_id'], convert['code'])
		return id_imported
	def router_import(self, convert, product, products_ext):
		return response_success('product_import')
	def before_product_import(self, convert, product, products_ext):
		return response_success()
	def product_import(self, convert, product, products_ext):
		return response_success()
	def after_product_import(self, product_id, convert, product, products_ext):
		return response_success()
	def addition_product_import(self, convert, product, products_ext):
		return response_success()
	# TODO: CUSTOMER

	# -----------------------------------------------------------------
	def prepare_customers_import(self):
		query = {
			'type': 'query',
			'query': "ALTER TABLE _DBPRF_customer MODIFY COLUMN password varchar(255)"
		}
		self.import_data_connector(query, 'customer')
		return self

	def prepare_customers_export(self):
		return self

	def get_customers_main_export(self):
		id_src = self._notice['process']['customers']['id_src']
		limit = self._notice['setting']['customers']
		query = {
			'type': 'select',
			'query': "SELECT * FROM  _DBPRF_customers WHERE customers_id > " + to_str(id_src) + " ORDER BY customers_id ASC LIMIT " + to_str(limit)
		}

		customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not customers or customers['result'] != 'success':
			return response_error('could not get customers main to export')
		return customers

	def get_customers_ext_export(self, customers):
		# url_query = self.get_connector_url('query')
		customers_ids = duplicate_field_value_from_list(customers['data'], 'customers_id')
		customer_ext_queries = {
			'address_book': {
				'type': 'select',
				'query': "SELECT * FROM  address_book WHERE customers_id IN " + self.list_to_in_condition(customers_ids),
			},
			'customers_info': {
				'type': 'select',
				'query': "SELECT * FROM  customers_info WHERE customers_info_id IN " + self.list_to_in_condition(customers_ids),
			}
		}
		customers_ext = self.select_multiple_data_connector(customer_ext_queries)
		if not customers_ext or customers_ext['result'] != 'success':
			return response_error()
		return customers_ext

	def convert_customer_export(self, customer, customers_ext):
		customer_data = self.construct_customer()
		customer_data['id'] = customer['customers_id']
		# customer_data['address']=customer['customers_default_address_id']
		# customer_data['group_id'] = customer['customer_group_id']
		customer_data['email'] = customer['customers_email_address']
		customer_data['password'] = customer['customers_password']
		customer_data['first_name'] = customer['customers_firstname']
		customer_data['last_name'] = customer['customers_lastname']
		customer_data['gender'] = customer['customers_gender']
		customer_data['dob'] = customer['customers_dob']
		customer_data['is_subscribed'] = customer['customers_newsletter']
		customer_data['fax'] = customer['customers_fax']
		customer_data['active'] = True

		customer_info = get_row_from_list_by_field(customers_ext['data']['customers_info'], 'customers_info_id', customer['customers_id'])
		if customer_info:
			customer_data['created_at'] = customer_info['customers_info_date_account_created']
			customer_data['updated_at'] = customer_info['customers_info_date_account_last_modified']
		if customer_data['dob'] == '0000-00-00 00:00:00':
			customer_data['dob'] = customer_data['created_at']
		address_books = get_row_from_list_by_field(customers_ext['data']['address_book'], 'customers_id', customer['customers_id'])
		if address_books:
			address_data = self.construct_customer_address()
			address_data['id'] = address_books['address_book_id']
			address_data['first_name'] = address_books['entry_firstname']
			address_data['last_name'] = address_books['entry_lastname']
			# address_data['gender'] = address_book['entry_gender']
			address_data['address_1'] = get_value_by_key_in_dict(address_books, 'entry_street_address', '')
			address_data['city'] = address_books['entry_city']
			address_data['postcode'] = address_books['entry_postcode']
			address_data['company'] = address_books['entry_company']
			address_data['gender'] = address_books['entry_gender']
			address_data['country']['id'] = address_books['entry_country_id']
			address_data['zone'] = address_books['entry_zone_id']
			address_data['state'] = ''
			address_data['telephone'] = customer['customers_telephone']
			# if address_book['address_book_id'] == customer['customers_default_address_id']:
			address_data['default']['billing'] = True
			address_data['default']['shipping'] = True
			customer_data['address'].append(address_data)
		return response_success(customer_data)

	def get_customer_id_import(self, convert, customer, customers_ext):
		return customer['customers_id']

	def check_customer_import(self, convert, customer, customers_ext):
		return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id'], convert['code']) else False

	def router_customer_import(self, convert, customer, customers_ext):
		return response_success('customer_import')
# ---------------------------------------------------------------------
	def before_customer_import(self, convert, customer, customers_ext):
		return response_success()

	def customer_import(self, convert, customer, customers_ext):
		return response_success(0)

	def after_customer_import(self, customer_id, convert, customer, customers_ext):
		return response_success()

	def addition_customer_import(self, convert, customer, customers_ext):
		return response_success()

	# TODO: ORDER
	def prepare_orders_import(self):
		return self

	def prepare_orders_export(self):
		return self
# -------------------------------------------------------------------------
	def get_orders_main_export(self):
		id_src = self._notice['process']['orders']['id_src']
		limit = self._notice['setting']['orders']
		query = {
			'type': 'select',
			'query': "SELECT * FROM  _DBPRF_orders WHERE orders_id > " + to_str(
				id_src) + " ORDER BY orders_id ASC LIMIT " + to_str(limit)
		}
		orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not orders or orders['result'] != 'success':
			return response_error('could not get orders main to export')
		return orders

	def get_orders_ext_export(self, orders):
		url_query = self.get_connector_url('query')
		order_ids = duplicate_field_value_from_list(orders['data'], 'orders_id')
		bil_country = duplicate_field_value_from_list(orders['data'], 'billing_country')
		delivery_country = duplicate_field_value_from_list(orders['data'], 'delivery_country')
		country_ids = set(bil_country + delivery_country)

		payment_zone = duplicate_field_value_from_list(orders['data'], 'bill_state')
		shipping_zone = duplicate_field_value_from_list(orders['data'], 'delivery_state')
		# cus_zone = duplicate_field_value_from_list(orders['data'], 'customers_state')
		state_ids = set(payment_zone + shipping_zone)
		cus_ids = duplicate_field_value_from_list(orders['data'], 'cust_id')
		orders_ext_queries = {
			'orders_products': {
				'type': 'select',
				'query': "SELECT * FROM  _DBPRF_orders_products WHERE orders_id IN " + self.list_to_in_condition(order_ids)
			},
			'orders_total': {
				'type': 'select',
				'query': "SELECT * FROM ` _DBPRF_orders_total` WHERE orders_id IN " + self.list_to_in_condition(order_ids)
			},
		}
		orders_ext = self.get_connector_data(url_query, {'serialize': True, 'query': json.dumps(orders_ext_queries)})
		if not orders_ext or orders_ext['result'] != 'success':
			return response_error()
		return orders_ext

	def convert_order_export(self, order, orders_ext):
		order_data = self.construct_order()
		order_data['id'] = order['orders_id']
		order_data['status'] = order['orders_status']
		order_data['tax']['title'] = 'Tax'
		order_data['tax']['amount'] = get_value_by_key_in_dict(order, 'total_tax', 0.0000)
		order_data['shipping']['title'] = 'Shipping'
		order_data['shipping']['amount'] = get_value_by_key_in_dict(order, 'total_ship', 0.0000)
		order_data['discount']['title'] = 'Discount'
		order_data['discount']['amount'] = 0.0000
		order_data['total']['title'] = 'Total'
		order_data['total']['amount'] = get_value_by_key_in_dict(order, 'order_total', 0.0000)
		order_data['subtotal']['title'] = 'Total products'
		# order_data['subtotal']['amount'] = get_value_by_key_in_dict(order, 'total', 0.0000)
		order_data['currency'] = order['currency']

		# currency = get_row_value_from_list_by_field(orders_ext['data']['currencies'], 'code', order['currency'], 'currencies_id')
		order_data['currency_value'] = order['currency_value']
		order_data['created_at'] = order['date_purchased']
		order_data['updated_at'] = order['last_modified']
		billing_name_split = order['billing_name'].split()
		delivery_name_split = order['delivery_name'].split()
		order_customer = self.construct_order_customer()
		# order_customer = self.add_c(order_customer)
		order_customer['id'] = order['client_customers_id']
		order_customer['email'] = order['customers_email']
		order_customer['first_name'] = delivery_name_split[0]
		order_customer['last_name'] = delivery_name_split[1]
		order_data['customer'] = order_customer

		customer_address = self.construct_order_address()
		customer_address['first_name'] = delivery_name_split[0]
		customer_address['last_name'] = delivery_name_split[1]
		customer_address['address_1'] = order['delivery_address1']
		customer_address['address_2'] = order['delivery_address2']
		customer_address['city'] = order['delivery_city']
		customer_address['postcode'] = order['delivery_postcode']
		customer_address['telephone'] = order['customers_telephone']
		customer_address['company'] = order['delivery_company']
		customer_address['country']['name'] = order['delivery_country']
		customer_address['state']['name'] = order['delivery_state']
		order_data['customer_address'] = customer_address

		order_billing = self.construct_order_address()
		# order_billing = this->addConstructDefault(order_billing)
		# billing_name = self.get_name_from_string(order['bill_fname'] + ' '+ order['bill_lname'])
		order_billing['first_name'] = billing_name_split[0]
		order_billing['last_name'] = billing_name_split[1]
		order_billing['address_1'] = order['billing_address1']
		order_billing['address_2'] = order['billing_address1']
		order_billing['city'] = order['billing_city']
		order_billing['postcode'] = order['billing_postcode']
		order_billing['telephone'] = order['customers_telephone']
		order_billing['company'] = order['billing_company']
		order_billing['country']['name'] = order['billing_country']
		order_billing['state']['name'] = order['billing_state']

		order_data['billing_address'] = order_billing

		order_delivery = self.construct_order_address()
		order_delivery['first_name'] = delivery_name_split[0]
		order_delivery['last_name'] = delivery_name_split[1]
		order_delivery['address_1'] = order['delivery_address1']
		order_delivery['address_2'] = order['delivery_address2']
		order_delivery['city'] = order['delivery_city']
		order_delivery['postcode'] = order['delivery_postcode']
		order_delivery['telephone'] = order['customers_telephone']
		order_delivery['company'] = order['delivery_company']
		order_delivery['state']['name'] = order['delivery_state']
		order_delivery['country']['name'] = order['delivery_country']

		order_delivery = self._cook_shipping_address_by_billing(order_delivery, order_billing)
		order_data['shipping_address'] = order_delivery
		# payments = get_row_from_list_by_field(orders_ext['data']['orders_payment'], 'order_id', order['id'])
		order_payment = self.construct_order_payment()
		order_payment['title'] = 'Payment'
		order_payment['method'] = order['payment_method']
		order_data['payment'] = order_payment

		order_products = get_list_from_list_by_field(orders_ext['data']['orders_products'], 'orders_id', order['orders_id'])
		order_items = list()
		for order_product in order_products:
			order_item_subtotal = to_decimal(order_product['products_price']) * to_decimal(order_product['products_quantity'])
			order_item_tax = to_decimal(order_item_subtotal) * to_decimal(8.250) / 100
			order_item_total = to_decimal(order_item_subtotal) + to_decimal(order_item_tax)
			order_item = self.construct_order_item()
			order_item['id'] = order_product['orders_id']
			order_item['product']['id'] = order_product['orders_products_id']
			order_item['product']['name'] = order_product['products_name']
			order_item['product']['sku'] = order_product['products_upc_code']
			order_item['qty'] = order_product['products_quantity']
			order_item['price'] = order_product['products_price']
			order_item['subtotal'] = order_item_subtotal
			order_item['total'] = order_item_total
			order_items.append(order_item)
		order_data['items'] = order_items
		order_data['subtotal']['tax'] = order_item_tax
		order_data['subtotal']['amount'] = order_item_subtotal
		order_data['total']['amount'] = order_item_total
		return response_success(order_data)

	def get_order_id_import(self, convert, order, orders_ext):
		return order['orders_id']

	def check_order_import(self, convert, order, orders_ext):
		return True if self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code']) else False

	def router_order_import(self, convert, order, orders_ext):
		return response_success('order_import')
# -----------------------------------------------------------------------
	def before_order_import(self, convert, order, orders_ext):
		return response_success()

	def order_import(self, convert, order, orders_ext):
		return response_success(0)

	def after_order_import(self, order_id, convert, order, orders_ext):
		return response_success()

	def addition_order_import(self, convert, order, orders_ext):
		return response_success()

	# TODO: REVIEW
	def prepare_reviews_import(self):
		return self

	def prepare_reviews_export(self):
		return self
# --------------------------------------------------------------------
	def get_reviews_main_export(self):
		id_src = self._notice['process']['reviews']['id_src']
		limit = self._notice['setting']['reviews']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_reviews WHERE reviews_id > " + to_str(
				id_src) + " ORDER BY reviews_id ASC LIMIT " + to_str(limit)
		}
		reviews = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not reviews or reviews['result'] != 'success':
			return response_error('could not get manufacturers main to export')
		return reviews

	def get_reviews_ext_export(self, reviews):
		url_query = self.get_connector_url('query')
		reviews_id = duplicate_field_value_from_list(reviews['data'], 'reviews_id')
		product_ids = duplicate_field_value_from_list(reviews['data'], 'products_id')
		reviews_ext_queries = {
			'products_description': {
				'type': 'select',
				'query': "SELECT products_id, language_id, products_name FROM _DBPRF_products_description WHERE products_id IN " + self.list_to_in_condition(
					product_ids)
			},
			'reviews_description': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_reviews_description WHERE reviews_id IN " + self.list_to_in_condition(
					reviews_id)
			},
		}
		reviews_ext = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(reviews_ext_queries)
		})

		if not reviews_ext or reviews_ext['result'] != 'success':
			return response_warning()
		return reviews_ext

	def convert_review_export(self, review, reviews_ext):
		review_data = self.construct_review()
		default_language = self._notice['src']['language_default']
		review_data['id'] = review['reviews_id']
		review_data['language_id'] = default_language
		review_description = get_row_from_list_by_field(reviews_ext['data']['reviews_description'], 'reviews_id', review['reviews_id'])
		if not review_description:
			return response_warning(self.warning_import_entity('Review', review['reviews_id'], None, 'Review data not exists.'))
		language_id = review_description['languages_id'] if review_description['languages_id'] else default_language
		product_descriptions = get_list_from_list_by_field(reviews_ext['data']['products_description'], 'products_id', review['products_id'])
		product_description = get_row_from_list_by_field(product_descriptions, 'language_id', language_id)
		if not product_description:
			product_description = get_row_from_list_by_field(product_descriptions, 'language_id', default_language)
		rv_status = {
			0: 2,  # pedding
			1: 1,  # approved
			3: 2  # not approved
		}
		review_data['language_id'] = language_id
		review_data['product']['id'] = review['products_id']
		review_data['product']['name'] = product_description['products_name'] if product_description else ' '
		review_data['customer']['id'] = review['customers_id']
		review_data['customer']['name'] = review['customers_name']
		review_data['title'] = ' '
		review_data['content'] = review_description['reviews_text']
		review_data['status'] = get_value_by_key_in_dict(rv_status, to_int(review['reviews_status']), 1) if 'reviews_status' in review else 1
		review_data['created_at'] = review['date_added']
		review_data['updated_at'] = review['last_modified']

		rating = self.construct_review_rating()
		rating['rate_code'] = 'default'
		rating['rate'] = review['reviews_rating']
		review_data['rating'].append(rating)
		return response_success(review_data)

	def get_review_id_import(self, convert, review, reviews_ext):
		return review['reviews_id']

	def check_review_import(self, convert, review, reviews_ext):
		return True if self.get_map_field_by_src(self.TYPE_REVIEW, convert['id'], convert['code']) else False

	def router_review_import(self, convert, review, reviews_ext):
		return response_success('review_import')
# ---------------------------------------------------------------------
	def before_review_import(self, convert, review, reviews_ext):
		return response_success()

	def review_import(self, convert, review, reviews_ext):
		product_id = False
		if convert['product']['id'] or convert['product']['code']:
			product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['product']['id'],
			                                       convert['product']['code'])
		if not product_id:
			response_warning('Review ' + to_str(convert['id']) + ' import false. Product does not exist!')
		customer_id = 0
		if convert['customer']['id'] or convert['customer']['code']:
			customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['customer']['id'],
			                                        convert['customer']['code'])
			if not customer_id:
				customer_id = 0
		review_data = {
			'product_id': product_id,
			'customer_id': customer_id,
			'author': convert['customer']['name'] if convert['customer']['name'] else '',
			'rating': self.calculate_average_rating(convert['rating']),
			'date_added': convert['created_at'],
			'date_modified': convert['updated_at'],
			'status': 1 if convert['status'] else 0,
			'text': convert['content']
		}
		review_id = self.import_review_data_connector(self.create_insert_query_connector('review', review_data), True, convert['id'])
		if not review_id:
			response_warning('review id ' + to_str(convert['id']) + ' import false.')
			self.insert_map(self.TYPE_REVIEW, convert['id'], review_id, convert['code'])
		return response_success(review_id)

	def after_review_import(self, review_id, convert, review, reviews_ext):
		return response_success()

	def addition_review_import(self, convert, review, reviews_ext):
		return response_success()

	# TODO: PAGE
	def prepare_pages_import(self):
		return self

	def prepare_pages_export(self):
		return self

	def get_pages_main_export(self):
		return response_success()

	def get_pages_ext_export(self, pages):
		return response_success()

	def convert_page_export(self, page, pages_ext):
		return response_success()

	def get_page_id_import(self, convert, page, pages_ext):
		return False

	def check_page_import(self, convert, page, pages_ext):
		return False

	def router_page_import(self, convert, page, pages_ext):
		return response_success('page_import')

	def before_page_import(self, convert, page, pages_ext):
		return response_success()

	def page_import(self, convert, page, pages_ext):
		return response_success(0)

	def after_page_import(self, page_id, convert, page, pages_ext):
		return response_success()

	def addition_page_import(self, convert, page, pages_ext):
		return response_success()

	# TODO: BLOCK
	def prepare_blogs_import(self):
		return response_success()

	def prepare_blogs_export(self):
		return self

	def get_blogs_main_export(self):
		return self

	def get_blogs_ext_export(self, blocks):
		return response_success()

	def convert_blog_export(self, block, blocks_ext):
		return response_success()

	def get_blog_id_import(self, convert, block, blocks_ext):
		return False

	def check_blog_import(self, convert, block, blocks_ext):
		return False

	def router_blog_import(self, convert, block, blocks_ext):
		return response_success('block_import')

	def before_blog_import(self, convert, block, blocks_ext):
		return response_success()

	def blog_import(self, convert, block, blocks_ext):
		return response_success(0)

	def after_blog_import(self, block_id, convert, block, blocks_ext):
		return response_success()

	def addition_blog_import(self, convert, block, blocks_ext):
		return response_success()

	# todo: code opencart
	def _list_to_in_condition_product(self, products):
		if not products:
			return "('null')"
		products = list(map(self.escape, products))
		products = list(map(lambda x: to_str(x), products))
		res = "','product_id=".join(products)
		res = "('product_id=" + res + "')"
		return res

	def product_to_in_condition_seourl(self, ids):
		if not ids:
			return "('null')"
		result = "('product_id=" + "','product_id=".join([str(id) for id in ids]) + "')"
		return result

	def category_to_in_condition_seourl(self, ids):
		if not ids:
			return "('null')"
		result = "('category_id=" + "','category_id=".join([str(id) for id in ids]) + "')"
		return result

	def get_category_parent(self, category_id):
		query = {
			'type': 'select',
			'query': "SELECT * FROM  _DBPRF_categories WHERE categories_id = " + to_str(category_id)
		}
		categories = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not categories or categories['result'] != 'success':
			return response_error('could not get category parent to export')
		if categories and categories['data']:
			category = categories['data'][0]
			categories_ext = self.get_categories_ext_export(categories)
			category_convert = self.convert_category_export(category, categories_ext)
			return category_convert
		return response_error('could not get category parent to export')

	def ategory_parent(self, convert_parent):
		parent_exists = self.get_map_field_by_src(self.TYPE_CATEGORY, convert_parent['id'], convert_parent['code'])
		if parent_exists:
			return response_success(parent_exists)
		category = convert_parent['category']
		categories_ext = convert_parent['categories_ext']
		category_parent_import = self.category_import(convert_parent, category, categories_ext)
		self.after_category_import(category_parent_import['data'], convert_parent, category, categories_ext)
		return category_parent_import

	def nl2br(self, string, is_xhtml=True):
		if is_xhtml:
			return string.replace('\n', '<br />\n')
		else:
			return string.replace('\n', '<br>\n')

	def get_country_id(self, code, name):
		query = 'SELECT country_id FROM `_DBPRF_country` '
		if code:
			query = query + ' WHERE iso_code_2 = "' + to_str(code) + '"'
		elif name:
			query = query + ' WHERE name = "' + to_str(name) + '"'
		countries_query = {
			'type': 'select',
			'query': query
		}
		countries = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(countries_query)})
		if not countries or countries['result'] != 'success' or not countries['data']:
			return 0
		return countries['data'][0]['country_id']

	def get_state_id(self, code, name):
		query = 'SELECT zone_id FROM `_DBPRF_zone` '
		if code:
			query = query + ' WHERE code = "' + to_str(code) + '"'
		elif name:
			query = query + ' WHERE name = "' + to_str(name) + '"'
		zones_query = {
			'type': 'select',
			'query': query
		}
		zones = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(zones_query)})
		if not zones or zones['result'] != 'success' or not zones['data']:
			return 0
		return zones['data'][0]['zone_id']

	def calculate_average_rating(self, rates, default='default'):
		rate = get_row_from_list_by_field(rates, 'rate_code', default)
		if rate and 'rate' in rate:
			return rate['rate']
		rate_total = 0
		count = to_len(rates)
		for _rate in rates:
			rate_total = rate_total + to_decimal(_rate['rate'])
		average = to_decimal(rate_total / count)
		if average > 5:
			return 5
		else:
			return math.ceil(average)

	def get_name_from_string(self, value):
		result = dict()
		parts = value.split(' ')
		result['lastname'] = parts.pop()
		result['firstname'] = " ".join(parts)
		return result

	def _cook_shipping_address_by_billing(self, shipping_address, billing_address):
		for key, value in shipping_address.items():
			if key in {'country', 'state'}:
				for child_key, child_value in shipping_address[key].items():
					if not shipping_address[key][child_key]:
						shipping_address[key][child_key] = billing_address[key][child_key]
			else:
				if not shipping_address[key]:
					shipping_address[key] = billing_address[key]

		return shipping_address

	def convert_float_to_percent(self, value):
		return value * 100

	def get_con_store_select(self):
		select_store = self._notice['src']['languages_select'].copy()
		src_store = self._notice['src']['languages'].copy()
		if self._notice['src']['language_default'] not in select_store:
			select_store.append(self._notice['src']['language_default'])
		src_store_ids = list(src_store.keys())
		unselect_store = [item for item in src_store_ids if item not in select_store]
		select_store.append(0)
		if to_len(select_store) >= to_len(unselect_store):
			where = ' IN ' + self.list_to_in_condition(select_store) + ' '
		else:
			where = ' NOT IN ' + self.list_to_in_condition(unselect_store) + ' '

		return where

	def detect_seo(self):
		return 'default_seo'
	def categories_default_seo(self, category, categories_ext):
		result = list()
		type_seo = self.SEO_301
		category_url = get_list_from_list_by_field(categories_ext['data']['URIs'],'cat_id', category['id'])
		seo_cate = self.construct_seo_category()
		if category_url:
			for cate_url in category_url:
				seo_cate['request_path'] = cate_url['uri']
				seo_cate['default'] = True
				seo_cate['type'] = type_seo
				result.append(seo_cate)


		return result

	def products_default_seo(self, product, products_ext):
		result = list()
		type_seo = self.SEO_301
		category_url = get_list_from_list_by_field(products_ext['data']['URIs'], 'cat_id', product['id'])
		seo_cate = self.construct_seo_product()
		if category_url:
			for cate_url in category_url:
				seo_cate['request_path'] = cate_url['uri']
				seo_cate['default'] = True
				seo_cate['type'] = type_seo
				result.append(seo_cate)

		return result

	def to_url(self, name):
		new_name = re.sub(r"[^a-zA-Z0-9-. ]", '', name)
		new_name = new_name.replace(' ', '-')
		url = new_name.lower()
		return url
