from catscore.http.request import CatsRequest
from catscore.lib.logger import CatsLogging as logging

class DetailSite:
    def __init__(self, request: CatsRequest, url):
        """[summary]

        Arguments:
            request {CatsRequest} -- [description]
            url {[type]} -- [description]
        """
        self.url = url
        self.soup = request.get(url=url, response_content_type="html").content

    @property
    def introduction(self):
        return self.soup.find("pre", {"class": "introduction"}).text
    
    @property
    def review_score(self):
        return self.soup.find("div", {"class": "review_table"}).find("svg").find("text").text
        