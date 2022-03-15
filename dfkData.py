import json
import requests
import time
from multiprocessing import Manager, Pool
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo import DeleteMany
from pyhmy import account
import pandas as pd
import csv

# Header information to be used while scraping
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}

# API url for harmony blockchain
main_net = 'https://rpc.s0.t.hmny.io'
main_net_shard_0 = 'https://rpc.s0.t.hmny.io'

# initialize an empty token for allTokens variable
manager = Manager()
allTokens = manager.list()

# Max heroid upto which we wish to fetch hero data
maxHeroId = 400

# header row
titles = ['heroID', 'userName', 'walletAddr', 'mainClass', 'Level', 'Summoner', 'Assistant', 'Main class', 'Sub class', 'Profession',
          'Gender', 'Element', 'Xp', 'Level', 'Hp', 'Mp', 'Sp', 'Stamina', 'Summons', 'Stat boost1', 'Stat boost2',
          'Strength', 'Endurance', 'Wisdom', 'Vitality', 'Dexterity', 'Intelligence', 'Luck', 'Agility', 'Mining',
          'Gardening', 'Foraging', 'Fishing']

# Function that fetches ONE balance contained by an address
def fetchAccountBalance(walletAddr):
    balance = account.get_balance(walletAddr, endpoint=main_net)
    actualBalance = balance / pow(10, 18) # because DecimalPoint for harmony contract is 18
    return actualBalance

def getSingleTokenDetails(tokenAddr):
    # need to loop through all available tokens
    tokenDetail = {}
    for token in allTokens:
        if(token['address'] == tokenAddr):
            tokenDetail['address'] = token['address']
            tokenDetail['decimals'] = token['decimals']
            tokenDetail['symbol'] = token['symbol']
            tokenDetail['name'] = token['name']
            return tokenDetail
    return tokenDetail

# Function that will retrieve all the tokens that have been deployed in Harmony Net
def getAllHRC20TokensInHarmonyNet():
    global allTokens
    apiURL = f"https://explorer-v2-api.hmny.io/v0/erc20/"
    try:
        res = requests.get(apiURL, headers=headers)
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout, requests.TooManyRedirects) as e:
        print("There was an error processing the request. Please try again later")
        return
    
    if(res.status_code != 200):
        return
    
    jsonString = res.text
    allTokensJson = json.loads(jsonString)
    for tk in allTokensJson:
        allTokens.append(tk)
    return allTokens

# Function to retrieve the entire token details contained in a wallet
def fetchTokensInWalletAddress(walletAddr):
    apiURL = f"https://explorer-v2-api.hmny.io/v0/erc20/address/{walletAddr}/balances"
    try:
        res = requests.get(apiURL, headers=headers)
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout, requests.TooManyRedirects) as e:
        print("There was an error processing the request. Please try again later")
        return

    if(res.status_code != 200):
        return

    jsonString = res.text
    walletTokens = json.loads(jsonString)
    return walletTokens

# Function that will return a string listing tokens and its amount contained in a wallet addr
def getWalletTokenDetails(walletAddr):
    walletTokenDetails = ''

    # fetch all tokens contained in a wallet address
    walletTokens = fetchTokensInWalletAddress(walletAddr)
    try:
        assert walletTokens != None
    except AssertionError as e:
        print("No tokens contained in the wallet")
        return 'N/A'

    for wToken in walletTokens:
        actualBalance = 0
        tokenSymbol = ''
        containedBalance = wToken['balance']
        
        # additional layer of verification
        if(wToken['ownerAddress'] == walletAddr):
            singleTokenDetail = getSingleTokenDetails(wToken['tokenAddress'])
            
            # Processing for decimal points of single token
            if(singleTokenDetail != {}):
                tokenDecimalPoints = singleTokenDetail['decimals']
                actualBalance = int(containedBalance) / (pow(10,tokenDecimalPoints))
        
                # get the token symbol for that particular token
                tokenSymbol = singleTokenDetail['symbol']

        # create a string that contains token details in a wallet, separated by a ;
        walletTokenDetails += str(actualBalance) + ' ' + tokenSymbol + ';'

    return walletTokenDetails

# Function that fetches details of a hero
def fetchHeroes(heroId):
    url = f"https://terra.engineer/en/harmony/dfk/heros/{heroId}"
    try:
        res = requests.get(url, headers=headers)
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout, requests.TooManyRedirects) as e:
        print("There was an error processing the request. Continuing with the next iteration")
        return 
    
    if(res.status_code != 200):
        return

    soup = BeautifulSoup(res.text, 'html.parser')

    allTables = soup.select('div.col-6 tr')
    nftClass = soup.find('div', {'class': 'card-header bg-primary text-white'})
    heroCards = nftClass.find_all('span')

    nftMainClass = heroCards[0].text
    nftLevel = heroCards[1].text

    walletAdd = soup.findAll("a",{"itemprop":"item"})
    try:
        wallet = walletAdd[2]
        addr = wallet['href'].split('/')[-1]
        userName = wallet.text.split(':')[1]
    except IndexError:
        userName = "---"
        addr = "N/A"

    # Comment out the portion for token Data retrieval
    """
    tokenData = ''
    walletONEbalance = 0
    if(addr != "N/A"):
        tokenData = getWalletTokenDetails(addr)
        walletONEbalance = fetchAccountBalance(addr)
    """

    heroValues = [heroId, userName, addr, nftMainClass, nftLevel]

    # nftLevel --> Gen 0 validates that there is no summoner or assistant
    # thus these values should be NULL
    genZeroString = 'Gen 0'
    if(genZeroString in nftLevel):
        # hard-coded append None twice, one for Summoner ID, another for Assistant
        heroValues.append(None) 
        heroValues.append(None)

    for tdata in allTables:
        rowsData = tdata.find('td').text
        if rowsData.startswith('\n'):
            rowsData = rowsData.split('\n')[1]

        heroValues.append(rowsData)

    print(heroValues)

    with open("dfkHeroData.csv", 'a') as file:
        writer = csv.writer(file)
        writer.writerow(heroValues)

def mongoimport(csv_path, db_name, collection_name, db_url='localhost', db_port=27017):
    # db connection
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    collection_name = db[collection_name]

    # read from JSON file
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient='records'))

    # remove any pre-existing records
    collection_name.bulk_write([DeleteMany({})])

    # insert the JSON data into the table
    collection_name.insert_many(payload)
    dcount = collection_name.count_documents({})
    print("Records inserted: " + str(dcount))

def main():
    global allTokens

    # create a pool of 20 processes to run multiprocessing
    pool = Pool(processes=20)
    startTime = time.time()
    print("Program started at: ", startTime)

    # print("Fetching all tokens deployed on the Harmony net")
    # getAllHRC20TokensInHarmonyNet()

    # write the header row
    with open("dfkHeroData.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(titles)

    pool.map(fetchHeroes, range(1, maxHeroId))
    pool.close()

    endTime = time.time()

    # insert records into local mongodb
    mongoimport('dfkHeroData.csv', 'defiKingdoms', 'heroes')

    print("Program ended at: ", endTime)
    elapsedTime = endTime - startTime
    print("Program running time: ", elapsedTime, " seconds")
    
if __name__ == "__main__":
    main()
