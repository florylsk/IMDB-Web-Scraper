from bs4 import BeautifulSoup
import requests
import pandas as pd
import concurrent.futures
import re

#global variables
MAX_THREADS=2 #number of threads to execute concurrently
titles=[]
top_cast=[]
synopsies=[]
directors=[]
storylines=[]
genres=[]
release_dates=[]
languages=[]
production_companies=[]
runtimes=[]
certificates=[]


def getTitle(soup):
    _title = soup.find("h1", {"data-testid": "hero-title-block__title"})
    try:
        title=_title.getText()
    except:
        titles.append("Not available")
        return
    titles.append(title)

def getCast(soup):
    _cast=soup.findAll("a",{"data-testid": "title-cast-item__actor"})
    _casts=[]
    for elem in _cast:
        _casts.append(elem.getText())
    casts=', '.join(_casts)
    top_cast.append(casts)

def getSynopsis(soup):
    _synopsis= soup.find("span", {"data-testid": "plot-xl"})
    try:
        synopsis=_synopsis.getText()
    except:
        synopsies.append("Not available")
        return
    synopsies.append(synopsis)

def getDirector(soup):
    _director=soup.find("a",{"class":"ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})
    try:
        director=_director.getText()
    except:
        directors.append("Not available")
        return
    directors.append(director)


def getStoryline(soup):
    _storyline= soup.find("div", {"data-testid": "storyline-plot-summary"})
    try:
        storyline=_storyline.getText()
    except:
        storylines.append("Not available")
        return
    storylines.append(storyline)


def getGenre(soup):
    _genre=soup.find("li", {"data-testid": "storyline-genres"})
    try:
        ___genres=_genre.getText()
    except:
        genres.append("Not available")
        return
    __genres=re.sub('([A-Z])', r', \1', ___genres)
    _genres=re.sub(r'^\W*\w+\W*', '', __genres)
    genres.append(_genres)

def getReleaseDates(soup):
    __releasedate = soup.find("li", {"data-testid": "title-details-releasedate"})
    try:
        _releasedate = __releasedate.getText()
    except:
        release_dates.append("Not available")
        return
    _releasedate_= re.sub(r'^\W*\w+\W*', '', _releasedate)
    releasedate=_releasedate_.replace('date','')
    release_dates.append(releasedate)


def getLanguages(soup):
    ___languages = soup.find("li", {"data-testid": "title-details-languages"})
    try:
        __languages = ___languages.getText()
    except:
        languages.append("Not Available")
        return
    _languages= re.sub('([A-Z])', r', \1', __languages)
    language=_languages.replace(", Languages, ",'')
    languages.append(language)

def getProductionCompanies(soup):
    ___companies = soup.find("li", {"data-testid": "title-details-companies"})
    try:
        __companies = ___companies.getText()
    except:
        production_companies.append("Not Available")
        return
    if "Production company" in __companies:
        company = __companies.replace("Production company", '')
    else:
        company = __companies.replace("Production companies", '')
    production_companies.append(company)


def getRuntime(soup):
    ___runtimes = soup.find("li", {"data-testid": "title-techspec_runtime"})
    try:
        __runtimes = ___runtimes.getText()
    except:
        runtimes.append("Not available")
        return
    runtime = __runtimes.replace("Runtime", '')
    runtimes.append(runtime)


def getCertificate(soup):
    ___certificates = soup.find("li", {"data-testid": "storyline-certificate"})
    try:
         __certificates = ___certificates.get_text()
    except:
        certificates.append("Not Available")
        return
    certificate = __certificates.replace("Certificate", '')
    certificates.append(certificate)




def getData(_url):
    #execute get requests and call the functions to insert the data into the lists
    _response = requests.get(_url)
    _soup = BeautifulSoup(_response.text, 'lxml')
    getTitle(_soup)
    getCast(_soup)
    getSynopsis(_soup)
    getDirector(_soup)
    getStoryline(_soup)
    getGenre(_soup)
    getReleaseDates(_soup)
    getLanguages(_soup)
    getProductionCompanies(_soup)
    getRuntime(_soup)
    getCertificate(_soup)

def concurrent_downloads(story_urls):
    #choose the number of threads
    threads = min(MAX_THREADS, len(story_urls))
    #map the threads to the main get data function
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(getData, story_urls)

#read excel for URLs
df = pd.read_excel('MovieGenreIGC_v3.xlsx')
urls = df["Imdb Link"]
urls_test=urls.loc[1:500]
#call the function that calls the main function concurrently
concurrent_downloads(urls_test)
#create a dataframe and convert it to json to feed elasticsearch
dict_movies = {'Title':titles,'Top Cast':top_cast,'Synopsis':synopsies,'Director':directors,'Storyline':storylines,
        'Genres':genres,'Release Date':release_dates,'Language':languages,'Production Companies':production_companies,
        'Runtime':runtimes,'Certificate':certificates}

movies = pd.DataFrame(dict_movies)
print(movies)
with open('test_json','w') as f:
    f.write(movies.to_json(orient="records",lines=True))

