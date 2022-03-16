import time
import json
import requests
from multiprocessing import Manager, Pool
from bs4 import BeautifulSoup
from pyhmy import account
import csv
from mangodbtest import add_hero_info_to_db

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
maxHeroId = 100

# header row
titles = ['heroID', 'userName', 'walletAddr', 'tokenData', 'walletONEbalance', 'mainClass', 'Level', 'Summoner', 'Assistant', 'Main class', 'Sub class', 'Profession',
          'Gender', 'Element', 'Xp', 'Level', 'Hp', 'Mp', 'Sp', 'Stamina', 'Summons', 'Stat boost1', 'Stat boost2',
          'Strength', 'Endurance', 'Wisdom', 'Vitality', 'Dexterity', 'Intelligence', 'Luck', 'Agility', 'Mining',
          'Gardening', 'Foraging', 'Fishing']

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
        if (wToken['ownerAddress'] == walletAddr):
            singleTokenDetail = getSingleTokenDetails(wToken['tokenAddress'])

            # Processing for decimal points of single token
            if (singleTokenDetail != {}):
                tokenDecimalPoints = singleTokenDetail['decimals']
                actualBalance = int(containedBalance) / (pow(10, tokenDecimalPoints))

                # get the token symbol for that particular token
                tokenSymbol = singleTokenDetail['symbol']

        # create a string that contains token details in a wallet, separated by a ;
        walletTokenDetails += str(actualBalance) + ' ' + tokenSymbol + ';'

    return walletTokenDetails
