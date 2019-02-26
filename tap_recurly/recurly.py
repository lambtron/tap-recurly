
#
# Module dependencies.
#

from datetime import datetime, timedelta
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
    self.uri = "https://{api_key}:@partner-api.recurly.com/".format(api_key=api_key)


  def sleep_until(self, timestamp=None):
    difference_in_seconds = timestamp - int(time.time())
    time.sleep(difference_in_seconds)


  def check_rate_limit(self, rate_limit_remaining=None, rate_limit_reset=None):
    if int(rate_limit_remaining) <= self.quota_rate_limit:
      self.sleep_until(rate_limit_reset)


  @backoff.on_exception(backoff.expo,
                        requests.exceptions.RequestException)
  def _get(self, path, **kwargs):
    uri = "{uri}{path}".format(uri=self.uri, path=path)
    logger.info("GET request to {uri}".format(uri=uri))
    response = requests.get(uri, headers=self.headers)
    response.raise_for_status()
    self.check_rate_limit(response.headers.get('X-RateLimit-Remaining'), response.headers.get('X-RateLimit-Reset'))
    print(response.json())
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
    return self._get_all("sites/{site_id}/accounts?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))


  # substream of accounts
  def billing_info(self, column_name=None, bookmark=None):
    return self._get_all("sites/{site_id}/accounts/{account_id}/billing_info?limit={limit}&sort={column_name}".format(site_id=self.site_id, account_id=account_id, limit=self.limit, column_name=column_name))

  
  # substream of accounts
  def adjustments(self, column_name=None, bookmark=None):
    return #


  # substream of accounts
  def accounts_coupon_redemptions(self, column_name=None, bookmark=None):
    return # self._get_all("/sites/{site_id}/accounts/{account_id}/coupon_redemptions?limit={limit}&sort={column_name}".format(site_id=self.site_id, account_id=account_id, limit=self.limit, column_name=column_name))


  def coupons(self, column_name=None, bookmark=None):
    return self._get_all("/sites/{site_id}/coupons?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))


  def invoices(self, column_name=None, bookmark=None):
    return self._get_all("/sites/{site_id}/invoices?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))


  # substream of invoices
  def invoices_coupon_redemptions(self, column_name=None, bookmark=None):
    return #


  def plans(self, column_name=None, bookmark=None):
    return self._get_all("/sites/{site_id}/plans?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))


  # substream of plans
  def plans_add_ons(self, column_name=None, bookmark=None):
    return #


  def subscrptions(self, column_name=None, bookmark=None):
    return self._get_all("/sites/{site_id}/subscriptions?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))


  def transactions(self, column_name=None, bookmark=None):
    return self._get_all("/sites/{site_id}/transactions?limit={limit}&sort={column_name}".format(site_id=self.site_id, limit=self.limit, column_name=column_name))





