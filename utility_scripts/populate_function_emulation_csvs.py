#!/usr/bin/env python3

"""
Scrapes the PG docs along with each specified vendor doc and creates
CSV mappings between our emulations and the functions.
"""



from bs4 import BeautifulSoup
import os
import requests

class PGScraper:

    subindex_urls=[
'https://www.postgresql.org/docs/12/functions-string.html',
'https://www.postgresql.org/docs/12/functions-math.html',
'https://www.postgresql.org/docs/12/functions-binarystring.html',
'https://www.postgresql.org/docs/12/functions-formatting.html',
'https://www.postgresql.org/docs/12/functions-datetime.html',
'https://www.postgresql.org/docs/12/functions-comparison.html',
'https://www.postgresql.org/docs/12/functions-enum.html',
'https://www.postgresql.org/docs/12/functions-geometry.html',
'https://www.postgresql.org/docs/12/functions-net.html',
'https://www.postgresql.org/docs/12/functions-textsearch.html',
'https://www.postgresql.org/docs/12/functions-json.html',
'https://www.postgresql.org/docs/12/functions-sequence.html',
'https://www.postgresql.org/docs/12/functions-array.html',
'https://www.postgresql.org/docs/12/functions-range.html',
'https://www.postgresql.org/docs/12/functions-aggregate.html',
'https://www.postgresql.org/docs/12/functions-window.html',
'https://www.postgresql.org/docs/12/functions-srf.html',
'https://www.postgresql.org/docs/12/functions-info.html',
'https://www.postgresql.org/docs/12/datatype-numeric.html'
    ]

    def __init__(self):
        self.get_pg_methods()
        self.get_snowshu_emulations()

    @staticmethod
    def find_with_url(methods,val,index=None):
        res=methods.get(val,None)
        if res is not None:
            res = res[index] if index is not None else res
            return "`{val} <{res}>`__".format(val=val,res=res)
        else:
            return "not supported"
    @staticmethod
    def find_comment(methods,val,index=None):
        res=methods.get(val,None)
        if res is not None:
            res = res[index] if index is not None else res
            return "{res}".format(res=res)
        else:
            return ""

    def get_pg_methods(self): 
        pg=self.scrape('https://www.postgresql.org/docs/current/sql-commands.html')
        self.pg_methods=dict(self.uppers([(sql.text.strip(),f'https://www.postgresql.org/docs/current/{sql["href"]}',) \
                            for sql in pg.find_all("div", class_="toc")[0].find_all('a', href=True)]))

        def subindexes(url):
            url_scrape=self.scrape(url)
            tuples=[]
            for table in url_scrape.find_all('table'):
                for func in table.find_all('code',class_=['function','literal']):
                    tuples.append((func.text.split('(')[0].strip(),url,))
            tuples=self.uppers(tuples)
            for k,v in tuples:
                self.pg_methods[k]=v
        [subindexes(index) for index in self.subindex_urls]

    def get_snowshu_emulations(self):
        pg_function_filepath='snowshu/adapters/target_adapters/postgres_adapter/functions/'
        all_files=[filename for filename in os.walk(pg_function_filepath)][0][2]
        function_files=[f for f in filter((lambda x : x.endswith('.sql')),all_files)]
        function_comments=[]
        for function in function_files:
            in_comment=False
            with open(pg_function_filepath+function,'r') as f:
                comment=''
                for line in f.readlines():
                    if '/*' in line:
                        in_comment=True
                    if in_comment:
                        comment +=line
                    if '*/' in line:
                        in_comment=False
                        continue      
                function_comments.append(comment.replace('/*','').replace('*/',''))



        function_keys=[f for f in map(lambda x : x[:-4], function_files)]
        bb_prefix='https://bitbucket.org/healthunion/snowshu/src/master/snowshu/adapters/target_adapters/functions/'
        function_urls=[f for f in map(lambda x : bb_prefix+x, function_files)]
        self.snowshu_emulations=dict(zip(function_keys, zip(function_urls,function_comments)))

    def scrape(self,url):
        return BeautifulSoup(requests.get(url).text)

    def uppers(self,iterable):
        return set(map((lambda x : (x[0].upper(),x[1],)),iterable))

### SNOWFLAKE ###
def main():

    scraper=PGScraper()

    snowflake=scraper.scrape('https://docs.snowflake.net/manuals/sql-reference/functions-all.html')
    chained=[(sql.text.strip(),f'https://docs.snowflake.net/manuals/sql-reference/{sql.parent["href"]}',) \
                     for sql in snowflake.find_all("table")[0].find_all('span',class_="doc")]
    unchained=set()
    for text,url in chained:
        splits=text.split(',')
        degenerated={(sp,url,) for sp in splits}
        unchained=unchained.union(degenerated)
    unchained=set(map((lambda x : (x[0].replace('[ NOT ]','').strip(),x[1],)),unchained))
    snowflake_methods=dict(scraper.uppers(filter((lambda x : 'Functions' not in x[0]),unchained)))
    meshed=[]
    in_order=sorted(snowflake_methods.items(),key=lambda x :x[0])
    
    


    for k,v in in_order:
        row='`{snowflake_function} <{snowflake_url}>`__,{postgres_val},{emulation_val},"{comment_val}"\n'.format(snowflake_function=k,
                                                                                                       snowflake_url=v,
                                                                                                       postgres_val=(scraper.find_with_url(scraper.pg_methods,k)),
                                                                                                       emulation_val=(scraper.find_with_url(scraper.snowshu_emulations,k,0)),
                                                                                                       comment_val=(scraper.find_comment(scraper.snowshu_emulations,k,1)))
        meshed.append(row)
    
    with open('docs/source/snowflake_function_map.csv','w') as f:
        f.write('"Snowflake Function","Postgresql Function","Replica Emulation","Notes"\n')
        for row in meshed:
            f.write(row)
        
if __name__ == '__main__':
    main()
