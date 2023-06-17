from bs4 import BeautifulSoup
import requests
import csv
from concurrent.futures import ThreadPoolExecutor
import re

headers = {
  "Host" : "www.wsj.com",
  "Referer" : "https://www.wsj.com",
  "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  "User-Agent" : "Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0",
  }
links = []
urlCA = "https://www.wsj.com/market-data/quotes/company-list/country/canada/"
research = "/research-ratings"
none = "N/A"

def get_links(url):
    a = True
    pageNum = 0
    while a:
        pageNum += 1
        pageContent = requests.get(url + str(pageNum),  allow_redirects=False, headers=headers).text
        stockLinks = BeautifulSoup(pageContent, 'html.parser').find('table').find_all('a')
        if stockLinks:
            for link in stockLinks:
                links.append(link.get('href'))
        else:
            a = False
    return links

def append_research_info(link):
    with open('wsjAnalystRatings.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        print(link)
        pageContent = requests.get(link + research,  allow_redirects=False, headers=headers).text
        researchPage = BeautifulSoup(pageContent, 'html.parser')
        stockAnalystRatings = researchPage.find("div", {"class": "cr_analystRatings"})
        if stockAnalystRatings:
            noData = stockAnalystRatings.find("span", {"class": "data_none"})
            if not noData:
                stockAnalystRatings = stockAnalystRatings.find("table").find_all("span", {"class": "data_data"})
                writer.writerow([link.split("/")[-1], stockAnalystRatings[2].text, stockAnalystRatings[5].text, stockAnalystRatings[8].text, stockAnalystRatings[11].text, stockAnalystRatings[14].text])
    file.close()

def append_financial_info(link):
    with open('wsjFinancialInfo.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        stockInfo = []
        print(link)
        pageContent = requests.get(link,  allow_redirects=False, headers=headers).text
        overviewPage = BeautifulSoup(pageContent, 'html.parser')
        keyStockInfo = overviewPage.select('div[class*="WSJTheme--cr_keystock"]')[0].select('span[class*="WSJTheme--data_data"], span[class*="WSJTheme--data_none"]')
        for data in keyStockInfo:
            stockInfo.append(data.text)
        if stockInfo[0] != 'N/A':
            stockInfo[0] = stockInfo[0][:stockInfo[0].find('.')+3]
        if len(stockInfo[5]) > 17:
            stockInfo[5] = "N/A"
        else:
            stockInfo[5] = stockInfo[5][:stockInfo[5].find('.')+4]
        writer.writerow([link.split("/")[-1], stockInfo[2], stockInfo[0], stockInfo[1], stockInfo[5]])
    file.close()


def main():
    links = get_links(urlCA)

    with open('wsjAnalystRatings.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow(["Stock Symbol", "Buy", "Overweight", "Hold", "Underweight", "Sell"])
    file.close()

    with open('wsjFinancialInfo.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow(["Stock Symbol", "Market Cap", "P/E Ratio", "EPS", "Yield"])
    file.close()

    # with ThreadPoolExecutor(max_workers=3) as threader:
    #     threader.map(append_research_info, links)

    with ThreadPoolExecutor(max_workers=1) as threader:
        threader.map(append_financial_info, links)


if __name__ == '__main__':
	main()