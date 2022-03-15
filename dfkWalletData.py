import json
import time
import requests
from functools import partial
from multiprocessing import Pool
from pymongo import MongoClient
from pyhmy import account
import pandas as pd
import csv


# API url for harmony blockchain
main_net = 'https://rpc.s0.t.hmny.io'
main_net_shard_0 = 'https://rpc.s0.t.hmny.io'

# initialize an empty token for allTokens variable
allTokens = []

# Header information to be used while scraping
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}

# Header row for wallet tokens
titles = ['walletAddr', 'tokensData', 'walletONEBalance']

def getWalletAddresses():
    # db connection
    client = MongoClient('mongodb://localhost:27017')
    db = client['defiKingdoms']
    heroesTable = db['heroes']

    # get distinct wallet addresses from heroes collection
    walletAddrs = db.heroes.distinct('walletAddr')
    return walletAddrs
    

def fetchAccountBalance(walletAddr):
    balance = account.get_balance(walletAddr, endpoint=main_net)
    actualBalance = balance / pow(10, 18) # because DecimalPoint for harmony contract is 18
    return actualBalance

def getSingleTokenDetails(tokenAddr, allTokens):
    
    tokenDetail = {}

    # loop through all available tokens
    for token in allTokens:
        if(token['address'] == tokenAddr):
            tokenDetail['address'] = token['address']
            tokenDetail['decimals'] = token['decimals']
            tokenDetail['symbol'] = token['symbol']
            tokenDetail['name'] = token['name']
            return tokenDetail
    return tokenDetail

def getWalletTokenDetails(walletAddr, allTokens):
    walletTokenDetails = ''

    walletTokenData = []

    # fetch all tokens contained in a wallet address
    walletTokens = fetchTokensInWalletAddress(walletAddr)

    # fetch the balance of a given wallet address
    balance = fetchAccountBalance(walletAddr)
    
    try:
        assert walletTokens != None
    except AssertionError as e:
        print("No tokens contained in the wallet")
        return 'N/A'


    for wToken in walletTokens:
        actualBalance = 0
        containedBalance = wToken['balance']
        
        # additional layer of verification
        if(wToken['ownerAddress'] == walletAddr):
            singleTokenDetail = getSingleTokenDetails(wToken['tokenAddress'], allTokens)
            
            # Processing for decimal points of single token
            if(singleTokenDetail != {}):
                tokenDecimalPoints = singleTokenDetail['decimals']
                actualBalance = int(containedBalance) / (pow(10,tokenDecimalPoints))
        # get the token symbol for that particular token
        tokenSymbol = singleTokenDetail['symbol']

        # create a string that contains token details in a wallet, separated by a ;
        walletTokenDetails += str(actualBalance) + ' ' + tokenSymbol + ';'

    walletTokenData = [walletAddr, walletTokenDetails, balance]
    print(walletTokenData)

    # write the wallet data into csv file
    # with open("dfkWalletData.csv", 'a') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(walletTokenData)

    # return walletTokenData


def getAllHRC20TokensInHarmonyNet():
    apiURL = f"https://explorer-v2-api.hmny.io/v0/erc20/"
    try:
        res = requests.get(apiURL, headers=headers)
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout, requests.TooManyRedirects) as e:
        print("There was an error processing the request. Please try again later")
        return
    
    if(res.status_code != 200):
        return
    
    jsonString = res.text
    allTokens = json.loads(jsonString)
    return allTokens

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


def main():

    # write the header row
    with open("dfkWalletData.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(titles)


    pool = Pool(processes=20)
    startTime = time.time()
    
    print("Program started at: ", startTime)
    print("Fetching all tokens deployed on the Harmony main network")
    allTokens = getAllHRC20TokensInHarmonyNet()

    # Fetch wallet addresses from heroes collection
    walletAddrs = getWalletAddresses()
    print("Total count of Wallet Address: ", len(walletAddrs))

    # define an intermediary function using partial because pool.map doeesn't allow multiple arguments
    getWalletTokenDetailsSettingAllTokens = partial(getWalletTokenDetails, allTokens=allTokens)

    walletData = pool.map(getWalletTokenDetailsSettingAllTokens, walletAddrs)
    print(walletData)
    pool.close()

    endTime = time.time()
    print("Program ended at ", endTime) 

    elapsedTime = endTime - startTime
    print("Program running time: ", elapsedTime, " seconds")   



if __name__ == "__main__":
    main()