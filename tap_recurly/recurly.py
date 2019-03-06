
#
# Module dependencies.
#

from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from singer import utils
import backoff
import requests
import logging
import time
import sys


logger = logging.getLogger()


""" Simple wrapper for Recurly. """
class Recurly(object):

  def __init__(self, start_date=None, user_agent=None, subdomain=None, api_key=None, quota_limit=50):
    self.headers = {'Accept': 'application/vnd.recurly.v2018-08-09'}
    self.site_id = "subdomain-{subdomain}".format(subdomain=subdomain)
    self.user_agent = user_agent
    self.start_date = start_date
    self.limit = 200
    self.total_rate_limit = 6000
    self.quota_rate_limit = int(quota_limit * self.total_rate_limit / 100)
    self.api_key = api_key
    self.uri = "https://partner-api.recurly.com/".format(api_key=api_key)


  def sleep_until(self, timestamp=None):
    difference_in_seconds = int(timestamp) - time.time()
    logger.info("Sleeping {seconds} seconds until {timestamp}".format(seconds=difference_in_seconds, timestamp=timestamp))
    time.sleep(difference_in_seconds)


  def check_rate_limit(self, rate_limit_remaining=None, rate_limit_reset=None):
    if int(rate_limit_remaining) <= self.quota_rate_limit:
      self.sleep_until(rate_limit_reset)


  @backoff.on_exception(backoff.expo,
                        requests.exceptions.RequestException)
  def _get(self, path, **kwargs):
    uri = "{uri}{path}".format(uri=self.uri, path=path)
    logger.info("GET request to {uri}".format(uri=uri))
    response = requests.get(uri, headers=self.headers, auth=HTTPBasicAuth(self.api_key, ''))
    response.raise_for_status()
    self.check_rate_limit(response.headers.get('X-RateLimit-Remaining'), response.headers.get('X-RateLimit-Reset'))
    return response.json()


  def _get_all(self, path, **kwargs):
    has_more = True
    while has_more:
      json = self._get(path)
      has_more = json["has_more"]
      path = json["next"]
      data = json["data"]
      for item in data:
        yield item

  # 
  # Methods to retrieve data per stream/resource.
  # 

  def accounts(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/accounts?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  # substream of accounts
  def billing_info(self, column_name=None, bookmark=None):
    accounts = self.accounts(column_name, bookmark)
    for account in accounts:
      for item in self._get_all("sites/{site_id}/accounts/{account_id}/billing_info?limit={limit}&sort={column_name}&order=asc".format(site_id=self.site_id, account_id=account["id"], limit=self.limit, column_name=column_name)):
        yield item

  
  def adjustments(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/line_items?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  # substream of accounts
  def accounts_coupon_redemptions(self, column_name=None, bookmark=None):
    accounts = self.accounts(column_name, bookmark)
    for account in accounts:
      for item in self._get_all("sites/{site_id}/accounts/{account_id}/coupon_redemptions?limit={limit}&sort={column_name}&order=asc".format(site_id=self.site_id, account_id=account["id"], limit=self.limit, column_name=column_name)):
        yield item


  def coupons(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/coupons?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  def invoices(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/invoices?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  # substream of invoices
  def invoices_coupon_redemptions(self, column_name=None, bookmark=None):
    invoices = self.invoices(column_name, bookmark)
    for invoice in invoices:
      for item in self._get_all("sites/{site_id}/invoices/{invoice_id}/coupon_redemptions?limit={limit}&sort={column_name}&order=asc".format(site_id=self.site_id, invoice_id=invoice["id"], limit=self.limit, column_name=column_name)):
        yield item


  def plans(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/plans?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  # substream of plans
  def plans_add_ons(self, column_name=None, bookmark=None):
    plans = self.plans(column_name, bookmark)
    for plan in plans:
      for item in self._get_all("sites/{site_id}/plans/{plan_id}/add_ons?limit={limit}&sort={column_name}&order=asc".format(site_id=self.site_id, plan_id=plan["id"], limit=self.limit, column_name=column_name)):
        yield item


  def subscriptions(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/subscriptions?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))


  def transactions(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/transactions?limit={limit}&sort={column_name}&begin_time={bookmark}&order=asc".format(site_id=self.site_id, limit=self.limit, column_name=column_name, bookmark=bookmark))





