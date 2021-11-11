from bs4 import BeautifulSoup
import requests
import pandas as pd
import concurrent.futures
import re
from tqdm import tqdm
import json


#global variables
MAX_THREADS=6 #number of threads to execute concurrently
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
ratings=[]
countries_origin=[]
plot_keywords=[]
years=[]
writers=[]
budgets=[]
grosses=[]
imagelinks=[]

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
    _languages = re.sub('([A-Z])', r', \1', __languages)
    if "Languages" in _languages:
        language=_languages.replace(", Languages, ",'')
    else:
        language = _languages.replace(", Language, ", '')
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



def getRating(soup):
    __ratings = soup.find("div", {"data-testid": "hero-rating-bar__aggregate-rating__score"})
    try:
        _ratings = __ratings.get_text()
    except:
        ratings.append("Not Available")
        return
    rating = _ratings.replace("/10", '')
    ratings.append(rating)


def getCountriesOrigin(soup):
    ___countries = soup.find("li", {"data-testid": "title-details-origin"})
    try:
        __countries = ___countries.getText()
    except:
        production_companies.append("Not Available")
        return
    if "Country of origin" in __countries:
        country = __countries.replace("Country of origin", '')
    else:
        country = __countries.replace("Countries of origin", '')
    countries_origin.append(country)


def getPlotKeywords(soup):
    __keywords = soup.find("div", {"data-testid": "storyline-plot-keywords"})
    try:
        _keywords = __keywords.get_text()
    except:
        plot_keywords.append("Not Available")
        return
    keyword= re.sub('([A-Z])', r' \1', _keywords)
    plot_keywords.append(keyword)



def getYear(soup):
    __years=soup.find("a",{"class":"ipc-link ipc-link--baseAlt ipc-link--inherit-color TitleBlockMetaData__StyledTextLink-sc-12ein40-1 rgaOW"})
    try:
        _years=__years.get_text()
    except:
        years.append("Not Available")
        return
    years.append(_years)


def getWriter(soup):
    ___writers=soup.findAll("li",{"data-testid":"title-pc-principal-credit"})
    try:
        __writers=___writers[1].get_text()
    except:
        writers.append("Not Available")
    if "Writers" in __writers:
        _writers=__writers.replace("Writers", '')
        writer=re.sub('([A-Z])', r' \1', _writers)
    else:
        _writers=__writers.replace("Writer", '')
        writer=_writers
    writers.append(writer)


def getBudget(soup):
    __budgets=soup.find("li",{"data-testid":"title-boxoffice-budget"})
    try:
        _budgets=__budgets.get_text()
    except:
        budgets.append("Not Available")
        return
    budget=_budgets.replace("Budget","")
    budget = budget.replace(" (estimated)", "")
    budgets.append(budget)


def getGross(soup):
    __grosses = soup.find("li", {"data-testid": "title-boxoffice-cumulativeworldwidegross"})
    try:
        _grosses = __grosses.get_text()
    except:
        grosses.append("Not Available")
        return
    gross = _grosses.replace("Gross worldwide", "")
    grosses.append(gross)


def getImages(soup):
    __imagelink=soup.find("a",{"aria-label":"View {Title} Poster"})
    try:
        _imagelink=__imagelink['href']
    except:
        imagelinks.append("Not Available")
        return
    _imagelink_ ="http://imdb.com"+_imagelink
    response_image=requests.get(_imagelink_)
    soup_image=BeautifulSoup(response_image.text,'lxml')
    imagelink=soup_image.find("img",{"class":"MediaViewerImagestyles__PortraitImage-sc-1qk433p-0 bnaOri"})
    try:
        imagelink_final=imagelink['src']
    except:
        imagelinks.append("Not Available")
        return
    imagelinks.append(imagelink_final)





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
    getRating(_soup)
    getCountriesOrigin(_soup)
    getPlotKeywords(_soup)
    getYear(_soup)
    getWriter(_soup)
    getBudget(_soup)
    getGross(_soup)
    getImages(_soup)

def concurrent_downloads(story_urls):
    #choose the number of threads
    threads = min(MAX_THREADS, len(story_urls))
    #map the threads to the main get data function
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        list(tqdm(executor.map(getData, story_urls),total=len(story_urls)))
#read excel for URLs
df = pd.read_excel('MovieGenreIGC_v3.xlsx')
urls = df["Imdb Link"]
urls_test=urls.loc[1:200] #Comment this line to get all the movies from the excel
#call the function that calls the main function concurrently
concurrent_downloads(urls_test)
#create a dataframe and convert it to json to feed elasticsearch
dict_movies = {'Title':titles,'Year':years,'Genres':genres,'Runtime':runtimes,'Language':languages,
        'Synopsis':synopsies,'Release Date':release_dates,'Storyline':storylines,'Production Companies':production_companies,
        'Director':directors,'Writers':writers,'Rating':ratings,'Country of Origin':countries_origin,'Plot Keywords':plot_keywords,
         'Top Cast':top_cast,'Certificate':certificates,'Budget':budgets,'Gross Worldwide':grosses,'Image':imagelinks}

movies = pd.DataFrame(dict_movies)
print(movies)

with open('movies.json','w',encoding='utf-8') as f:
    json=json.dumps(movies.to_dict('records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())