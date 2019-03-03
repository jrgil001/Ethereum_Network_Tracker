import requests
import json
from flask import Flask
from flask import jsonify
import datetime
import os

app = Flask(__name__)

@app.route("/")
def home():
    ethLatestBlock = getEthLatestBlock()
    ethLatestBlockNumber = getDecimalFromHex(ethLatestBlock['number'])
    print("ethLatestBlockNumber:" + str(ethLatestBlockNumber))

    getEthLastNumberOfBlocks(99, ethLatestBlockNumber)
    # getEthLastNumberOfBlocks(ethLatestBlockNumber, ethLatestBlockNumber)

    return getJsonFromDictionary(ethLatestBlock)

# getEthLastNumberOfBlocks returns the previous 'numPreviousBlocks' blocks from a 'numBlock' block given
def getEthLastNumberOfBlocks(numPreviousBlocks, numBlock):
    currentDate = createFolderByDate()
    i = numBlock-numPreviousBlocks    
    while i <= numBlock:
        ethBlock = getEthBlockByNumber(getHexFromDecimal(i))
        createFileForBlock(ethBlock, currentDate)
        createFileForEachTransactionAndReceipt(ethBlock, currentDate)
        i += 1

    return True

# createFolderByDate creates an ew folder named with current date
def createFolderByDate():
    currentDate = datetime.date.today()
    return createFolder('./', currentDate)

# createFolder creates a new folder in the given path with the name sent 
def createFolder(path, folderName):
    if os.path.exists(path + str(folderName)) == False:
        os.makedirs(path + str(folderName))
    
    return path + str(folderName)

# createFileForEachTransactionAndReceipt writes a json file with the receipt data for each transaction in a block
def createFileForEachTransactionAndReceipt(ethBlock, currentDate):
    transactions = ethBlock['transactions']
    ethBlockNumber = getDecimalFromHex(ethBlock['number'])
    folderName = createFolder('./' + str(currentDate) + '/', ethBlockNumber)
    createFolder('./' + str(currentDate) + '/', str(ethBlockNumber) + '_rec')
    writeTransactionAndReceipts(transactions, folderName)

# writeTransactionReceipts returns the receipt data for the transactions in the folder given
def writeTransactionAndReceipts(transactions, folderName):
    for transaction in transactions:
        writeFile(transaction, folderName, False)
        writeFile(getTransactionReceipt(transaction['hash']), str(folderName) + '_rec', True)

# writeFile returns the receipt data for the transactions in the folder given
def writeFile(transaction, folderName, isReceipt):
    if isReceipt:
        transactionHash = transaction['transactionHash']
        transaction['contractAddress'] = str(transaction['contractAddress'])
        #TODO logs contains different format the generates errors when converting to JSON
        transaction['logs'] = []
        transaction['logsBloom'] = []
    else:
        transactionHash = transaction['hash']
    f = open('./' + str(folderName) + '/' + str(transactionHash) + str(getExtension(isReceipt)) + ".json","w+")
    f.write(str(transaction))
    f.close()

#getExtension returns the '_rec' extension for the file name when is receipt
def getExtension(isReceipt):
    if isReceipt:
        return '_rec'
    else:
        return ''

# getTransactionReceipt returns the receipt data for the transaction hash given
def getTransactionReceipt(transactionHash):    
    requestData = getRequestData('eth_getTransactionReceipt', '["'+ str(transactionHash) +'"]')
    return executeInfuraRequest(requestData)

# createFileForBlock writes the data from a specific block into a json file
def createFileForBlock(ethBlock, currentDate):
    ethBlockNumber = getDecimalFromHex(ethBlock['number'])
    print("writing ethBlockNumber:" + str(ethBlockNumber))
    f = open('./' + str(currentDate) + '/' + str(ethBlockNumber) + ".json","w+")
    f.write(str(ethBlock))
    f.close()
    print("Finished ethBlockNumber:" + str(ethBlockNumber))

# getEthLatestBlock returns the ethereum latest block with JSON format
def getEthLatestBlock():
    return getEthBlockByNumber("latest")

# getEthBlock returns the ethereum block based on a block number given with JSON format
def getEthBlockByNumber(blockNumber):
    requestData = getRequestData('eth_getBlockByNumber', '["'+ str(blockNumber) +'",true]')
    return executeInfuraRequest(requestData)

# executeInfuraRequest returns the data retrieved from Infura based on the requested method and params
def executeInfuraRequest(requestData):
    header = {"Content-type": "application/json"}
    print("executing..."+requestData)
    response = requests.post("https://mainnet.infura.io/v3/your_infura_identification", data=requestData, headers=header)
    return getDataFromResponse(response)

# getDataFromResponse returns the JSON data in the response
def getDataFromResponse(response):
    responseJson = response.json()
    return responseJson['result']

# getRequestData returns the body of the http response
def getRequestData(method, params):
    requestData='{"jsonrpc":"2.0","method":"'+ method +'","params": '+ params +', "id":1}'
    return requestData

# getDecimalFromHex converts from HEX to Decimal numbers
def getDecimalFromHex(hexNumber):
    return int(hexNumber, 16)

# getHexFromDecimal converts from Decimal to HEX numbers
def getHexFromDecimal(decNumber):
    return hex(decNumber)

# getJsonFromDictionary converts a dictionary into JSON // useful for flask app, it isn't possible to return dictionaries
def getJsonFromDictionary(dictionary):
    return jsonify(dictionary)
