import json
import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup
from threading import Thread
import csv
from pyhmy import account

# API url for harmony blockchain
main_net = 'https://rpc.s0.t.hmny.io'
main_net_shard_0 = 'https://rpc.s0.t.hmny.io'

# initialize an empty token for allTokens variable
allTokens = []

# Header information to be used while scraping
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}

def fetchAccountBalance(walletAddr):
    balance = account.get_balance(walletAddr, endpoint=main_net)
    actualBalance = balance / pow(10, 18) # because DecimalPoint for harmony contract is 18
    print("Balance of addr {addr} is {bal}".format(addr=walletAddr, bal=actualBalance))
    return actualBalance

def getTransactionHistory(walletAddr):
    last_10_txn_hashes = account.get_transaction_history(walletAddr, page=0, page_size=10, include_full_tx=False, endpoint=main_net, order='DESC')
    return last_10_txn_hashes

def getSingleTokenDetails(tokenAddr):
    # need to loop through all available tokens
    global allTokens
    tokenDetail = {}
    for token in allTokens:
        if(token['address'] == tokenAddr):
            tokenDetail['address'] = token['address']
            tokenDetail['decimals'] = token['decimals']
            tokenDetail['symbol'] = token['symbol']
            tokenDetail['name'] = token['name']
            return tokenDetail
    return tokenDetail

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
        containedBalance = wToken['balance']
        
        # additional layer of verification
        if(wToken['ownerAddress'] == walletAddr):
            singleTokenDetail = getSingleTokenDetails(wToken['tokenAddress'])
            print(singleTokenDetail)
            # Processing for decimal points of single token
            if(singleTokenDetail != {}):
                tokenDecimalPoints = singleTokenDetail['decimals']
                actualBalance = int(containedBalance) / (pow(10,tokenDecimalPoints))
        # get the token symbol for that particular token
        tokenSymbol = singleTokenDetail['symbol']

        print(str(actualBalance) + ' ' + tokenSymbol + ';')

        # create a string that contains token details in a wallet, separated by a ;
        walletTokenDetails += str(actualBalance) + ' ' + tokenSymbol + ';'

    return walletTokenDetails

            

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
    print("walletTokens are: ", walletTokens)
    return walletTokens
    


def main():

    # TO-DO: Run a thread that fetches all the the tokens deployed in harmony net
    print("Fetching all tokens deployed on the Harmony main network")
    allTokens = getAllHRC20TokensInHarmonyNet()
    
    data = getWalletTokenDetails('0xdc48dcda07955a9c1a53c2e0d4163286764c4cbc')
    print(data)
    print(len(data))
    
    myData = getWalletTokenDetails('0x4e61ec8516bee87fcad8e30c349deead8f3184d5')
    print("My data is: ", myData)
    
    fetchAccountBalance('0x9c38D59A910C286004ca11821313BEc739c98c7f')
    
# Main boiler plate code
if __name__ == "__main__":
    main()