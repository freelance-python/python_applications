""" Importing python libraries """
import random
from logging import Logger
import logging
import time
from enum import Enum

""" Importing third party libraries """
from prefect import flow
from prefect import Task
from prefect.server.schemas.states import StateType
import requests
from requests import RequestException

""" Importing custom libraries """
from schemas import HodlHodlOfferBase
from schemas import HodlHodlUserBase


class Scraper_Names(Enum):
    hodlhodl = "hodlhodl"


class HodlhodlComScraper:
    def __init__(self, logger: Logger | None = None, proxy: dict = None, prefect: bool = False, **kwargs):
        """
        Initializing class variable some here
        1. proxy: that is set to None, but will be helpful in concurrent request.
        2. logger: defined to acess the log, however logs are not stored in any file or database, storing log will help in maintaining code in future.
        3. prefect: is a third party library that will help in data workflow, however in this case logger can be done using prefect.
        4. requester: is defined to requests's session, so caching the request will reduce load on server.
        5. BASE_URL: create base url, so it can be changed from here only if there is changes. for example in version like v1 to v2
        6. total_offer_percent_to_scrape: used to limit the request, but not used anywhere
        """
        self.proxy = None
        self.logger = logger 
        self.prefect = prefect
        # create a session object
        self.requester = requests.Session()
        self.BASE_URL = 'https://hodlhodl.com/api/'
        self.total_offer_percent_to_scrape = kwargs.get("total_offer_percent_to_scrape", 100)

    def get_currency_list(self):
        """"
        Here we are getting currencies list from hodlhodl API and creating a list of currencies code.
        """
        url = f'{self.BASE_URL}frontend/currencies'
        currency_list = []
        try:
            currencies = self.requester.get(url).json()
            for curr in currencies['currencies']:
                currency_list.append(curr.get("code"))
        except RequestException as e:
            self.logger.error("Error fetching currency list: %s", e)

        return currency_list

    def get_and_post_offers(self, curr, trading_type):
        """
        create_seller_data in this function as it not required
        in post api. May be earlier APIs required that, but now its accecpting 
        bearer token to authenticate. Howerer it is using to create object for
        HodlHodlUserBase in schemas 
        """
        url = f"{self.BASE_URL}frontend/offers?filters[currency_code]={curr}&pagination[offset]=0&filters[side]={trading_type}&facets[show_empty_rest]=true&facets[only]=false&pagination[limit]=100"
        try:
            resp = self.requester.get(url).json()
            for offer in resp.get("offers"):
                offer_info = self.create_offer_data(offer)
                seller_info = self.create_seller_data(offer)
                self.post_data_to_api(offer_info)
        except RequestException as e:
            self.logger.error("Error fetching offers: %s", e)

    def create_offer_data(self, offer):
        """ creating HodlHodlOfferBase object from offer data """
        return HodlHodlOfferBase(
            offer_identifier=offer.get("id"),
            fiat_currency=offer.get("asset_code"),
            country_code=offer.get("country_code"),
            trading_type_name=offer.get("side"),
            trading_type_slug=offer.get("side"),
            payment_method_name=offer.get("payment_methods")[0].get("type") if offer.get("payment_methods") else None,
            payment_method_slug=offer.get("payment_methods")[0].get("type") if offer.get("payment_methods") else None,
            description=offer.get("description"),
            currency_code=offer.get("currency_code"),
            coin_currency=offer.get("currency_code"),
            price=offer.get("price"),
            min_trade_size=offer.get("min_amount"),
            max_trade_size=offer.get("max_amount"),
            site_name='hodlhodl',
            margin_percentage=0,
            headline=''
        )

    def create_seller_data(self, offer):
        """ creating HodlHodlUserBase object from offer for user data """
        return HodlHodlUserBase(
            username=offer.get("trader").get("login"),
            feedback_score=offer.get("trader").get("rating") if offer.get("trader").get("rating") else 0,
            completed_trades=offer.get("trader").get("trades_count"),
            seller_url=offer.get("trader").get("url"),
            profile_image='',
            trade_volume=0
        )

    def post_data_to_api(self, offer_info):
        """
        Removed seller argument in this function as I saw there is not requiremt
        of it. refer to https://hodlhodl.com/api/docs#offers-creating-offer
        """
        offer_info = offer_info.dict()

        cc = offer_info["country_code"]
        if cc == "Global":
            cc = 'GL'
        
        offer_info['country_code'] = cc

        """ I don't think user's detail required in this post data according to docs of hodlhodl """
        data = {
            "offer": offer_info,
        }

        """ params are not required to prepaired so commenting these """

        """
        params = {
            "country_code": cc,
            "payment_method": offer_info.dict()["payment_method_name"],
            "payment_method_slug": offer_info.dict()["payment_method_slug"],
        }
        """

        try:
            return self.post_request_to_api(endpoint="v1/offers", data=data).json()
        except RequestException as e:
            self.logger.error("Error posting data to API: %s", e)
    

    """ for posting data it required api_key_required """
    def post_request_to_api(self, endpoint, data):
        """ creating post request to hodlhodl """
        URL = str(self.BASE_URL)+str(endpoint)
        res = self.requester.post(url=URL, data=data)
        return res

    def starter_cli(self):
        """ creating function for picking random currency fron get_currency_list and updating it to HodlhodlComScraper """
        currencies_list = self.get_currency_list()
        curr = random.choice(currencies_list)
        self.get_and_post_offers(curr, 'sell')

    def starter(self):
        """
        Two functions are not defined count_offers and get_counter.
        1. scheduling prefect task
        """
        currencies_list = self.get_currency_list()
        for curr in currencies_list:
            for trading_type in ['buy', 'sell']:
                if self.prefect:
                    rate = Task(self.get_and_post_offers, name=f"get hodlhodl offers").submit(curr, trading_type,
                                                                                             return_state=True)
                    if rate.type != StateType.COMPLETED or not rate.result():
                        self.logger.error('Task failed')
                        continue
                    count_offers(rate.result(), Scraper_Names.hodlhodl.name)
                    self.logger.debug("Got %s rates", rate)

                    offer_counter = get_counter(Scraper_Names.hodlhodl.name)
                    return offer_counter

                else:
                    self.get_and_post_offers(curr, trading_type)
            time.sleep(1)  # rate limiting

@flow
def get_hodlhodl_offers():
    """ initialing logger """
    logger = logging.getLogger()
    ag = HodlhodlComScraper(logger=logger)
    return ag.starter()


if __name__ == "__main__":
    """ calling a function on main.py call """
    get_hodlhodl_offers()

