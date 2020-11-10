'''
Example of web scraping using Python and BeautifulSoup.
The script will loop through a defined number of pages to extract mutual funds data.
'''

import datetime
import json
import os
import re
from urllib.parse import urlparse
import time
import requests
from requests import Session
from bs4 import BeautifulSoup
import concurrent.futures


class MFScrapper(object):

    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)

        self._search_url = config['search_url']
        self._file_name = config['file_name']
        self._cred_rating_url = config['cred_rating_url']
        self._output_path = config['output_path']
        self._mf_list_num = config['mf_list_num']
        self._cnt = 1
        self._get_session()

    def _get_session(self):
        time.sleep(1)
        session = Session()
        headers = self._get_headers()
        # Add headers
        session.headers.update(headers)
        self._session = session
        return session

    def _get_file_name(self):
        cur_date = datetime.datetime.now()
        date_name = cur_date.strftime("%d-%m-%Y")
        cur_file_name = self._file_name.format(date_name)
        return self._output_path + cur_file_name

    def write_output(self, row):
        with open(self._get_file_name(), 'a') as toWrite:
            json.dump(row, toWrite)
            toWrite.write('\n')

    # Staticmethod to extract a domain out of a normal link
    @staticmethod
    def parse_url(url):
        parsed_uri = urlparse(url)
        return parsed_uri

    def cast_value(self, value, type='float'):
        try:
            if value == '--':
                return 0
            elif type == 'int':
                return int(value)
            else:
                return float(value)
        except:
            return 0

    def _get_headers(self):
        host = self.parse_url(self._search_url)
        headers = {'Host': host.netloc,
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'}
        return headers

    def get_src_html(self, url):
        try:
            response = self._session.get(url)
            print("URL : {} \nTime taken : {}".format(url, response.elapsed.total_seconds()))
        except requests.exceptions.RequestException as e:
            print(e)
            exit()
        return response

    def get_listing_data(self):
        '''
        scrap search data from the page
        '''
        # prepare headers
        print("Listing URL : " + self._search_url)
        # fetching the url, raising error if operation fails
        response = self.get_src_html(self._search_url)
        listings = []
        #
        # loop through the table, get data from the columns
        if response.status_code == 200:
            soup = BeautifulSoup(response.json()['html_data'], "html.parser")
            tbody = soup.find("tbody")
            if tbody:
                links = tbody.find_all("tr")
                if len(links) != 0:
                    listings = links[:self._mf_list_num]
        return listings

    def parse_fund_details(self, html):
        try:
            if self._cnt % 10 == 0:
                self._get_session()
            details = {}
            sub_rows = html.find_all("td")
            if sub_rows:
                # print('\n Processing Item : ' + str(i))
                details['name'] = sub_rows[1].find('a').get_text()
                details['link'] = sub_rows[1].find("a")['href']
                rating = sub_rows[2].find('img')
                if rating:
                    details['rating'] = self.cast_value(rating.attrs.get('alt').replace(' star', ''), 'int')
                else:
                    details['rating'] = 0
                details['category'] = sub_rows[4].find('a').attrs['title']
                details['category_code'] = sub_rows[4].find('a').get_text()
                details['asset_value'] = self.cast_value(sub_rows[9].get_text().replace(",", ""), 'int')
                details['expense_ratio'] = self.cast_value(sub_rows[6].get_text())

                fund_details = self.get_fund_details(details['link'])
                details['fund_house'] = fund_details['fund_house'] if 'fund_house' in fund_details else ''
                details['risk'] = fund_details['risk'] if 'risk' in fund_details else ''
                details['risk_grade'] = fund_details['risk_grade'] if 'risk_grade' in fund_details else ''
                details['return_grade'] = fund_details['return_grade'] if 'return_grade' in fund_details else ''
                details['fund_growth'] = fund_details['fund_growth'] if 'fund_growth' in fund_details else {}
                details['cat_growth'] = fund_details['cat_growth'] if 'cat_growth' in fund_details else {}
                details['credit_rating'] = fund_details['credit_rating'] if 'credit_rating' in fund_details else {}
                details['top_holdings'] = fund_details['top_holdings'] if 'top_holdings' in fund_details else []
                details['fund_risk'] = fund_details['fund_risk'] if 'fund_risk' in fund_details else {}
                details['category_risk'] = fund_details['category_risk'] if 'category_risk' in fund_details else {}
                self._cnt += 1
                self.write_output(details)
        except Exception as e:
            print(e, details['link'], fund_details)
            exit()

    def get_fund_details(self, detail_url):
        '''
        scrap detail page data from the page
        '''
        fund_id = detail_url.split("/")[2]
        host = self.parse_url(self._search_url)
        detail_url = '{uri.scheme}://{uri.netloc}'.format(uri=host) + detail_url
        print(" Detail URL : " + detail_url)
        # prepare headers
        response = self.get_src_html(detail_url)
        fund_details = {}
        if response.status_code == 200:
            # print(response.text)
            soup = BeautifulSoup(response.text, "html.parser")
            fund_basic_details = self._get_basic_details(soup)
            trailing_returns = self._get_trailing_returns(soup)
            credit_rating = self._get_credit_rating(fund_id)
            top_holding = self._get_top_holdings(soup)
            risk_measures = self._get_risk_measures(soup)

            fund_details = {**fund_basic_details, **trailing_returns, **credit_rating, **top_holding, **risk_measures}
        return fund_details

    def _get_basic_details(self, src_html):
        try:
            temp = []
            details = {}
            # loop through the table, get data from the columns
            basic_details_table = src_html.find('table', id='basic-investment-table')
            if basic_details_table is not None:
                table = basic_details_table.find_all("tr")
                if table:
                    for rows in table:
                        sub_rows = rows.find_all("td")
                        if sub_rows:
                            temp.append(sub_rows[1])
                if temp:
                    fund_house = temp[0].find('a').get_text()
                    details['fund_house'] = re.sub('[^0-9a-zA-Z]+', ' ', fund_house).strip()
                    details['risk'] = temp[4].get_text()
                    details['risk_grade'] = temp[8].get_text().strip()
                    details['return_grade'] = temp[9].get_text().strip()
            return details
        except Exception as e:
            print(" Function : {}\n Exception : {}".format('_get_basic_details', e))

    def _get_trailing_returns(self, src_html):
        try:
            trailing_returns = {}
            ret_data = []
            trailing_returns_table = src_html.find('table', attrs={'id': 'trailing-returns-table'})
            if trailing_returns_table is not None:
                table_body = trailing_returns_table.find('tbody')
                rows = table_body.find_all('tr')
                if rows:
                    for row in rows:
                        cols = row.find_all('td')
                        cols = [ele.text.strip() for ele in cols]
                        ret_data.append([ele for ele in cols if ele])

                fund_data = {}
                fund_data['1y'] = self.cast_value(ret_data[0][7])
                fund_data['3y'] = self.cast_value(ret_data[0][8])
                fund_data['5y'] = self.cast_value(ret_data[0][9])
                fund_data['7y'] = self.cast_value(ret_data[0][10])
                fund_data['10y'] = self.cast_value(ret_data[0][11])
                trailing_returns['fund_growth'] = fund_data

                cat_ret = {}
                cat_ret['1y'] = self.cast_value(ret_data[2][7])
                cat_ret['3y'] = self.cast_value(ret_data[2][8])
                cat_ret['5y'] = self.cast_value(ret_data[2][9])
                cat_ret['7y'] = self.cast_value(ret_data[2][10])
                cat_ret['10y'] = self.cast_value(ret_data[2][11])
                trailing_returns['cat_growth'] = cat_ret

            return trailing_returns
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_trailing_returns', e))

    def _get_credit_rating(self, fund_id):
        try:
            credit_rating_data = {}
            cred_rating_url = self._cred_rating_url + fund_id
            print(" Credit Rating URL : " + cred_rating_url)

            response = self.get_src_html(cred_rating_url)
            i = 0
            cred_rating = {}
            if response.status_code == 200:
                for cat in response.json()['categories']:
                    cred_rating[cat] = self.cast_value(response.json()['series'][0]['data'][i])
                    i += 1
            credit_rating_data['credit_rating'] = cred_rating
            return credit_rating_data
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_credit_rating', e))

    def _get_top_holdings(self, src_html):
        try:
            top_holdings_data = {}
            top_holdings = []
            i = 0
            top_holdings_table = src_html.find('table', id='debt-holdings-table')
            if top_holdings_table:
                for rows in top_holdings_table.find('tbody').find_all("tr"):
                    if i > 10:
                        break
                    sub_rows = rows.find_all("td")
                    if sub_rows:
                        top_holdings.append(sub_rows[1].get_text().strip())
                        i += 1
            top_holdings_data['top_holdings'] = top_holdings
            return top_holdings_data
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_top_holdings', e))

    def _get_risk_measures(self, src_html):
        try:
            risk_measures = {}
            ret_data = []
            risk_measures_table = src_html.find('table', attrs={'id': 'risk-measures-table'})
            if risk_measures_table is not None:
                table_body = risk_measures_table.find('tbody')
                rows = table_body.find_all('tr')
                if rows:
                    for row in rows:
                        cols = row.find_all('td')
                        cols = [ele.text.strip() for ele in cols]
                        ret_data.append([ele for ele in cols if ele])
                # print(ret_data[0][1])
                # exit()
                fund_risk = {}
                fund_risk['mean'] = self.cast_value(ret_data[0][1])
                fund_risk['std_dev'] = self.cast_value(ret_data[0][2])
                fund_risk['sharpe'] = self.cast_value(ret_data[0][3])
                fund_risk['sortino'] = self.cast_value(ret_data[0][4])
                fund_risk['beta'] = self.cast_value(ret_data[0][5])
                fund_risk['alpha'] = self.cast_value(ret_data[0][6])
                risk_measures['fund_risk'] = fund_risk
                #
                cat_risk = {}
                cat_risk['mean'] = self.cast_value(ret_data[2][1])
                cat_risk['std_dev'] = self.cast_value(ret_data[2][2])
                cat_risk['sharpe'] = self.cast_value(ret_data[2][3])
                cat_risk['sortino'] = self.cast_value(ret_data[2][4])
                cat_risk['beta'] = self.cast_value(ret_data[2][5])
                cat_risk['alpha'] = self.cast_value(ret_data[2][6])
                risk_measures['category_risk'] = cat_risk
            return risk_measures
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_risk_measures', e))

    def extract(self):
        # Get mf data
        listings = self.get_listing_data()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.parse_fund_details, listings)
        print("Listings fetched successfully.")


# Main function
if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), '../configs/scrapper.json')
    mf = MFScrapper(config_path)
    mf.extract()
    # data = mf.get_fund_details('/funds/15680/axis-banking-and-psu-debt-fund-direct-plan')
    # print(data)
