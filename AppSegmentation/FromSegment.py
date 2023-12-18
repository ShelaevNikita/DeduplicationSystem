
import sqlite3 as SQL

from time import thread_time
from cryptography.fernet import Fernet

class FileFromSegment():
    
    def __init__(self, configParameters):     
        self.databaseFileName   = configParameters['binaryPath'] + \
                                  configParameters['databaseFileName'] + '.db'        
        self.secretKeyFileName  = configParameters['binaryPath'] + configParameters['secretKeyFileName']
        self.resultDataFileName = configParameters['resultPath'] + configParameters['dataFileName']       
        self.orderSegFileName   = configParameters['binaryPath'] + \
                                  configParameters['dataFileName'].split('.')[0] + '_Order.dat'
        
        self.separatorSegment   = configParameters['separatorSegment'].encode()
        self.separatorOrder     = configParameters['separatorOrder']
        
        self.idSegmentDict = {}

        self.orderIndex    = 0
        self.segmentIndex  = 0

        self.secretKey     = None
        self.connectionDB  = None
        self.cursorDB      = None
  
    def openAllFiles(self):
        flagContinue = True
        try:
            file = open(self.databaseFileName, 'r')
            file.close()

            file = open(self.secretKeyFileName, 'rb')
            file.close()
 
            file = open(self.orderSegFileName, 'r')
            file.close()
            
        except FileNotFoundError:
            print('\t ERROR: Input files not found...')
            flagContinue = False
            
        return flagContinue

    def makeConnectionBD(self):
        self.connectionDB = SQL.connect(self.databaseFileName)
        self.cursorDB = self.connectionDB.cursor()
        return

    def getSegmentFromFile(self, fileName, lineNumber, scale): 
        try:
            lineIndex = 0
            line = b''
            with open(fileName, 'rb') as fileData:
                while lineIndex <= lineNumber:
                    line = fileData.readline()
                    lineIndex += 1
            segment = line[:-1].split(self.separatorSegment)[scale]
            
        except FileNotFoundError:
            segment = b'ERROR'
            print('\t ERROR: File not found...')
            
        return segment

    def selectSegmentFromDB(self, segmentID):
        segment = b'ERROR'
        self.cursorDB.execute('''
            SELECT segCount, fileName, line, scale 
                FROM segment WHERE id = ?''', (segmentID, ))
        selectResult = self.cursorDB.fetchone()
        newSegCount = selectResult[0] - 1
        if newSegCount < 0:
            print('\t ERROR: Segment count < 0...')
            return segment
        
        self.cursorDB.execute('UPDATE segment SET segCount = ? WHERE id = ?', (newSegCount, segmentID))
        self.connectionDB.commit()
        segmentSecret = self.getSegmentFromFile(selectResult[1], selectResult[2], selectResult[3])
        if segmentSecret != segment:
            segment = Fernet(self.secretKey).decrypt(segmentSecret)
        return segment
    
    def saveSegmentToFile(self, segment):
        with open(self.resultDataFileName, 'ab') as fileData:
            fileData.write(segment)
        return

    def getSegment(self, orderID):
        if orderID in self.idSegmentDict:
            segment = self.idSegmentDict[orderID]
        else:
            segment = self.selectSegmentFromDB(orderID)
            self.idSegmentDict[orderID] = segment
            self.segmentIndex += 1        
        return segment

    def splitOrderLine(self, segmentIdList):
        idOrderList = segmentIdList.split(self.separatorOrder)
        
        for orderLine in idOrderList:
            orderIdArray = orderLine.split('-')
            orderID = int(orderIdArray[0])
            segment = self.getSegment(orderID)
            
            for _ in range(int(orderIdArray[1])):
                self.orderIndex += 1
                self.saveSegmentToFile(segment)
                
        return    

    def connectSegments(self):
        with open(self.orderSegFileName, 'r') as fileInput:
            while True:
                segmentIdList = fileInput.readline()
                if len(segmentIdList) == 0:
                    break
                self.splitOrderLine(segmentIdList[:-1])
        self.connectionDB.close()
        return

    def openSecretKey(self):
        with open(self.secretKeyFileName, 'rb') as fileData:
            self.secretKey = fileData.readline()
        return

    def main(self):
        print('\n START \"Collect segments into file\"...')
        start = thread_time()
        flagContinue = self.openAllFiles()
        if flagContinue:
            self.openSecretKey()
            self.makeConnectionBD()        
            self.connectSegments()
            finish = thread_time()
            print(f'\t Segment count = {self.orderIndex} ({self.segmentIndex} - unique)')
            print('\t Time process work =', str(finish - start), '(sec)')
        print(' END \"Collect segments into file\"...\n')
        return

if __name__ == '__main__':
    
    defaultConfigParameters = {
        'binaryPath'        :'./../outputBinary/',
        'resultPath'        :'./../outputData/',
        'databaseFileName'  :'SegmentDB',
        'secretKeyFileName' :'SecretKey.dat',
        'segmentStorePrefix':'SegmentList',
        'separatorSegment'  :'|',
        'separatorOrder'    :'|',
        'dataFileName'      :'9.txt'        
    }

    FileFromSegment(defaultConfigParameters).main()