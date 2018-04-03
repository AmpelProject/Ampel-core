#!/usr/local/bin/python
import urllib, urllib2,base64,pickle
from bs4 import BeautifulSoup
import re, os, datetime

marshal_root = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
scanning_url = marshal_root + 'growth_treasures_transient.cgi'
saving_url = marshal_root + 'save_cand_growth.cgi?candid=%s&program=%s'
annotate_url = marshal_root + 'edit_comment.cgi'
class PTFConfig(object) :
    def __init__(self) :
        import ConfigParser
        self.fname = os.path.expanduser('~/.ptfconfig.cfg')
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(self.fname)
    def get(self,*args,**kwargs) :
        return self.config.get(*args,**kwargs)
def get_marshal_html(weblink):
    request = urllib2.Request(weblink)
    conf = PTFConfig()
    base64string = base64.encodestring('%s:%s' % (conf.get('Marshal', 'user'), conf.get('Marshal', 'passw'))).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
    return urllib2.urlopen(request).read()

def soup_obj(url):
    return BeautifulSoup(get_marshal_html(url), 'lxml')

def save_source(candid, progid):
    return BeautifulSoup(get_marshal_html(saving_url %(candid, progid)), 'lxml') 

today = datetime.datetime.now().strftime('%Y-%m-%d')
class marshal_scanning(object):
    def __init__(self, program_name = 'Nuclear Transients',start_date = today, end_date = today) :
        soup = soup_obj(scanning_url)
        program_options = soup.findAll('select', {"name":"cutprogramidx"})[0]
        for x in program_options.findAll('option'):
            if x.text.strip() == program_name:
                self.cutprogramidx = x["value"]
        
        self.soup = soup_obj(scanning_url + "?cutprogramidx=%s&startdate=%s&enddate=%s" %(self.cutprogramidx, start_date, end_date))
        
        table = soup.findAll('table')
        self.table_rows = table[1].findAll('tr')[1:]
        for x in self.table_rows[0].findAll('td')[5].findAll('select')[0].findAll('option'):
            if x.text.strip() == program_name:
                self.program = x["value"]
    def list_sources(self):
        sources = []
        for source in self.table_rows:
            sources.append({})
            sources[-1]["candid"] = source.findAll('td')[5].findAll('input', {"name":'candid'})[0]["value"]
            for x in source.findAll('td')[5].findAll('b'):
                if x.text.strip() == 'ID:':
                    sources[-1]["name"] = x.next_sibling.strip()
                elif x.text.strip() == 'Coordinate:':
                    sources[-1]["ra"], sources[-1]["dec"] = x.next_sibling.split()
        
            for tag in self.table_rows[0].findAll('td')[-1].findAll('b'):
                key = tag.text.replace(u'\xa0', u'')
                sources[-1][key.strip(':')] = tag.next_sibling.strip()
        return sources


def annotate(comment,sourcename, comment_type="info"):
    soup = soup_obj(marshal_root + 'view_source.cgi?name=%s' %sourcename)
    cmd = {}
    for x in soup.find('form', {'action':"edit_comment.cgi"}).findAll('input'):
        if x["type"] == "hidden":
            cmd[x['name']] =x['value']
    cmd["comment"] = comment
    cmd["type"] = comment_type
    params = urllib.urlencode(cmd)
    return soup_obj(marshal_root + 'edit_comment.cgi?%s' %s)
