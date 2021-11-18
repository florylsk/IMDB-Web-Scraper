import pandas
from bs4 import BeautifulSoup
import requests
import pandas as pd
import concurrent.futures
import re
from tqdm import tqdm
import json


#global variables
MAX_THREADS=5 #number of threads to execute concurrently
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
    try:
        _title = soup.find("h1", {"data-testid": "hero-title-block__title"})
    except:
        titles.append("Not Available")
        return
    try:
        title=_title.getText()
    except:
        titles.append("Not available")
        return
    titles.append(title)

def getCast(soup):
    try:
        _cast=soup.findAll("a",{"data-testid": "title-cast-item__actor"})
    except:
        top_cast.append("Not Available")
        return
    _casts=[]
    for elem in _cast:
        try:
            _casts.append(elem.getText())
        except:
            top_cast.append("Not Available")
            return
    casts=', '.join(_casts)
    top_cast.append(casts)

def getSynopsis(soup):
    try:
        _synopsis= soup.find("span", {"data-testid": "plot-xl"})
    except:
        synopsies.append("Not Available")
        return
    try:
        synopsis=_synopsis.getText()
    except:
        synopsies.append("Not available")
        return
    synopsis = synopsis.replace('"', "'")
    synopsies.append(synopsis)

def getDirector(soup):
    try:
        _director=soup.find("a",{"class":"ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})
    except:
        directors.append("Not Available")
        return
    try:
        director=_director.getText()
    except:
        directors.append("Not available")
        return
    directors.append(director)


def getStoryline(soup):
    try:
        _storyline= soup.find("div", {"data-testid": "storyline-plot-summary"})
    except:
        storylines.append("Not Available")
        return
    try:
        storyline=_storyline.getText()
    except:
        storylines.append("Not available")
        return
    storyline = storyline.replace('"', "'")
    storylines.append(storyline)


def getGenre(soup):
    try:
        _genre=soup.find("li", {"data-testid": "storyline-genres"})
    except:
        genres.append("Not Available")
        return
    try:
        ___genres=_genre.getText()
    except:
        genres.append("Not available")
        return
    __genres=re.sub('([A-Z])', r', \1', ___genres)
    _genres=re.sub(r'^\W*\w+\W*', '', __genres)
    genres.append(_genres)

def getReleaseDates(soup):
    try:
        __releasedate = soup.find("li", {"data-testid": "title-details-releasedate"})
    except:
        release_dates.append("Not Available")
        return
    try:
        _releasedate = __releasedate.getText()
    except:
        release_dates.append("Not available")
        return
    _releasedate_= re.sub(r'^\W*\w+\W*', '', _releasedate)
    releasedate=_releasedate_.replace('date','')
    release_dates.append(releasedate)


def getLanguages(soup):
    try:
        ___languages = soup.find("li", {"data-testid": "title-details-languages"})
    except:
        languages.append("Not Available")
        return
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
    try:
        ___companies = soup.find("li", {"data-testid": "title-details-companies"})
    except:
        production_companies.append("Not Available")
        return
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
    try:
        ___runtimes = soup.find("li", {"data-testid": "title-techspec_runtime"})
    except:
        runtimes.append("Not Available")
        return
    try:
        __runtimes = ___runtimes.getText()
    except:
        runtimes.append("Not available")
        return
    runtime = __runtimes.replace("Runtime", '')
    runtimes.append(runtime)


def getCertificate(soup):
    try:
        ___certificates = soup.find("li", {"data-testid": "storyline-certificate"})
    except:
        certificates.append("Not Available")
        return
    try:
         __certificates = ___certificates.get_text()
    except:
        certificates.append("Not Available")
        return
    certificate = __certificates.replace("Certificate", '')
    certificates.append(certificate)



def getRating(soup):
    try:
        __ratings = soup.find("div", {"data-testid": "hero-rating-bar__aggregate-rating__score"})
    except:
        ratings.append("Not Available")
        return
    try:
        _ratings = __ratings.get_text()
    except:
        ratings.append("Not Available")
        return
    rating = _ratings.replace("/10", '')
    ratings.append(rating)


def getCountriesOrigin(soup):
    try:
        ___countries = soup.find("li", {"data-testid": "title-details-origin"})
    except:
        countries_origin.append("Not Available")
        return
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
    try:
        __keywords = soup.find("div", {"data-testid": "storyline-plot-keywords"})
    except:
        plot_keywords.append("Not Available")
        return
    try:
        _keywords = __keywords.get_text()
    except:
        plot_keywords.append("Not Available")
        return
    keyword= re.sub('([A-Z])', r' \1', _keywords)
    plot_keywords.append(keyword)



def getYear(soup):
    try:
        __years=soup.find("a",{"class":"ipc-link ipc-link--baseAlt ipc-link--inherit-color TitleBlockMetaData__StyledTextLink-sc-12ein40-1 rgaOW"})
    except:
        years.append("Not Available")
        return
    try:
        _years=__years.get_text()
    except:
        years.append("Not Available")
        return
    years.append(_years)


def getWriter(soup):
    try:
        ___writers=soup.findAll("li",{"data-testid":"title-pc-principal-credit"})
    except:
        writers.append("Not Available")
        return
    try:
        __writers=___writers[1].get_text()
    except:
        writers.append("Not Available")
        return
    __writers=__writers.replace('"',"'")
    if "Writers" in __writers:
        _writers=__writers.replace("Writers", '')
        writer=re.sub('([A-Z])', r' \1', _writers)
    else:
        _writers=__writers.replace("Writer", '')
        writer=_writers
    writers.append(writer)


def getBudget(soup):
    try:
        __budgets=soup.find("li",{"data-testid":"title-boxoffice-budget"})
    except:
        budgets.append("Not Available")
        return
    try:
        _budgets=__budgets.get_text()
    except:
        budgets.append("Not Available")
        return
    budget=_budgets.replace("Budget","")
    budget = budget.replace(" (estimated)", "")
    budgets.append(budget)


def getGross(soup):
    try:
        __grosses = soup.find("li", {"data-testid": "title-boxoffice-cumulativeworldwidegross"})
    except:
        grosses.append("Not Available")
        return
    try:
        _grosses = __grosses.get_text()
    except:
        grosses.append("Not Available")
        return
    gross = _grosses.replace("Gross worldwide", "")
    grosses.append(gross)


def getImages(soup):
    try:
        __imagelink=soup.find("a",{"aria-label":"View {Title} Poster"})
    except:
        imagelinks.append("Not Available")
        return
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
    if (_response.status_code==404):
        return
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
urls_final=[]
for e in urls:
    if (len(e)==35):
        urls_final.append(e)
    else:
     test=e.replace('tt0','tt')
     urls_final.append(test)

urls_test=urls_final[25000:] #Comment this line and type urls instead of urls_test in next function call to get all the movies from the excel
#call the function that calls the main function concurrently
concurrent_downloads(urls_test)
#create a dataframe and convert it to json to feed elasticsearch
dict_movies = {'Title':titles,'Year':years,'Genres':genres,'Runtime':runtimes,'Language':languages,
        'Synopsis':synopsies,'Release Date':release_dates,'Storyline':storylines,'Production Companies':production_companies,
        'Director':directors,'Writers':writers,'Rating':ratings,'Country of Origin':countries_origin,'Plot Keywords':plot_keywords,
         'Top Cast':top_cast,'Certificate':certificates,'Budget':budgets,'Gross Worldwide':grosses,'Image':imagelinks}

#movies = pd.DataFrame(dict_movies)
final_df=pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in dict_movies.items() ]))
#print(movies)

with open('movies_p9.json','w',encoding='utf-8') as f:
    json=json.dumps(final_df.to_dict(orient='records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())