
import sqlite3 as SQL
import hashlib as HSL

from time import thread_time
from cryptography.fernet import Fernet

class SegmentToFile():
    
    def __init__(self, configParameters):
        self.inputDataFileName   = configParameters['dataPath'] + configParameters['dataFileName']
        self.databaseFileName    = configParameters['binaryPath'] + \
                                   configParameters['databaseFileName'] + '.db'        
        self.secretKeyFileName   = configParameters['binaryPath'] + configParameters['secretKeyFileName']
        self.segmentListFileName = configParameters['binaryPath'] + \
                                   configParameters['segmentStorePrefix'] + '0.dat' 
        self.orderSegFileName    = configParameters['binaryPath'] + \
                                   configParameters['dataFileName'].split('.')[0] + '_Order.dat'
        
        self.segmentStorePrefix  = configParameters['segmentStorePrefix']
        self.segmentInLine       = configParameters['segmentInLine']
        self.orderInLine         = configParameters['orderInLine']
        self.separatorSegment    = configParameters['separatorSegment'].encode()
        self.separatorOrder      = configParameters['separatorOrder']
        self.segmentInOneFile    = configParameters['segmentInOneFile']
        self.blockSize           = configParameters['blockSize']
        
        self.segmentIdDict  = {}

        self.segmentCount   = 0
        self.orderIndex     = 0
        self.segmentIndex   = 0
        self.lastIndexID    = 0
        self.lastIndexCount = 0
        self.segmentFiles   = 0
        
        self.secretKey      = None       
        self.connectionDB   = None
        self.cursorDB       = None

    def createDB(self, fileNameFull):
        self.connectionDB = SQL.connect(fileNameFull)
        self.cursorDB = self.connectionDB.cursor()
        self.cursorDB.execute('''
            CREATE TABLE IF NOT EXISTS segment(
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                segCount INTEGER NOT NULL DEFAULT 0,
                fileName TEXT NOT NULL,
                line INTEGER NOT NULL DEFAULT 0,
                scale INTEGER NOT NULL DEFAULT 0)
            ''')
        self.connectionDB.commit()
        return
    
    def makeConnectionBD(self):
        fileNameFull = self.databaseFileName
        try:
            file = open(fileNameFull, 'r')
            file.close()
            self.connectionDB = SQL.connect(fileNameFull)
            self.cursorDB = self.connectionDB.cursor()
            
        except FileNotFoundError:
            self.createDB(fileNameFull)
            
        return

    def saveSegmentToFile(self, segment):
        dataToFile = Fernet(self.secretKey).encrypt(segment)
        self.segmentIndex += 1
        
        with open(self.segmentListFileName, 'ab') as fileData:
            fileData.write(dataToFile)
            if self.segmentIndex % self.segmentInLine > 0:
                fileData.write(self.separatorSegment)
            else:
                fileData.write(b'\n')
                
        if self.segmentIndex % self.segmentInOneFile == 0:
            self.segmentIndex  = 0
            self.segmentFiles += 1
            splitFileName = self.segmentListFileName.split('/')
            offsetPrefix  = len(self.segmentStorePrefix)
            newNumber = int(splitFileName[-1][offsetPrefix:].split('.')[0]) + 1
            self.segmentListFileName = '/'.join(splitFileName[:-1]) + '/' + \
                self.segmentStorePrefix + str(newNumber) + '.dat'
        return

    def saveOrderToFile(self, segmentID):
        if self.lastIndexID == 0:
            self.lastIndexID = segmentID
            self.lastIndexCount = 1
            return            

        if segmentID == self.lastIndexID:
            self.lastIndexCount += 1
            return
        
        self.orderIndex += 1
        
        with open(self.orderSegFileName, 'a') as fileData:
            fileData.write(str(self.lastIndexID) + '-' + str(self.lastIndexCount))
            if self.orderIndex % self.orderInLine > 0:
                fileData.write(self.separatorOrder)
            else:
                fileData.write('\n')
            self.lastIndexID = segmentID
            self.lastIndexCount = 1
        return

    def checkSegmentIntoDB(self, segment):
        segmentHash = HSL.sha256(segment).hexdigest()
        self.cursorDB.execute('SELECT id FROM segment WHERE hash = ?', (segmentHash, ))
        segmentID = 0
        selectResult = self.cursorDB.fetchone()      
        if selectResult is not None:
            segmentID = selectResult[0]
            self.segmentIdDict[segment] = segmentID
        return segmentID

    def updateSegmentInDB(self, segmentID):
        self.cursorDB.execute('SELECT segCount FROM segment WHERE id = ?', (segmentID, ))
        newSegCount = self.cursorDB.fetchone()[0] + 1
        self.cursorDB.execute('UPDATE segment SET segCount = ? WHERE id = ?', (newSegCount, segmentID))
        return

    def insertSegmentIntoDB(self, segment):
        line, scale = divmod(self.segmentIndex, self.segmentInLine)
        segmentHash = HSL.sha256(segment).hexdigest()
        self.cursorDB.execute('''
            INSERT INTO segment (hash, segCount, fileName, line, scale)
                VALUES (?, ?, ?, ?, ?)
            ''', (segmentHash, 1, self.segmentListFileName, line, scale))
        segmentID = self.cursorDB.lastrowid       
        self.segmentIdDict[segment] = segmentID
        self.saveSegmentToFile(segment)
        return segmentID

    def saveSegment(self, segment):
        if segment in self.segmentIdDict:
            segmentID = self.segmentIdDict[segment]
        else:
            segmentID = self.checkSegmentIntoDB(segment)
        if segmentID > 0:
            self.updateSegmentInDB(segmentID)
        else:
            segmentID = self.insertSegmentIntoDB(segment)
        self.connectionDB.commit()
        self.saveOrderToFile(segmentID)
        self.segmentCount += 1
        return

    def splitSegments(self):
        with open(self.inputDataFileName, 'rb') as fileInput:
            while True:
                segment = fileInput.read(self.blockSize)
                if len(segment) == 0:
                    break
                self.saveSegment(segment)
        self.connectionDB.close()
        self.saveOrderToFile(0)
        return

    def generateSecretKey(self):
        try:
            with open(self.secretKeyFileName, 'rb') as fileData:
                self.secretKey = fileData.readline()
                
        except FileNotFoundError:
            self.secretKey = Fernet.generate_key() 
            with open(self.secretKeyFileName, 'wb') as fileData:
                fileData.write(self.secretKey)
                
        return

    def checkSegmentIndex(self):
        self.cursorDB.execute('SELECT id FROM segment', ())
        selectResult = self.cursorDB.fetchall()
        if selectResult is None:
            self.segmentIndex = 0
        else:
            self.segmentIndex = len(selectResult)
        return self.segmentIndex

    def main(self):
        print('\n START \"Segmentation file\"...')
        start = thread_time()
        self.generateSecretKey()
        self.makeConnectionBD()
        selectResult = self.checkSegmentIndex()
        self.splitSegments()
        finish = thread_time()
        self.segmentIndex += (self.segmentFiles * self.segmentInOneFile) - selectResult
        print('\t File size =', self.segmentCount * self.blockSize)
        print(f'\t Segment count = {self.segmentCount} ({self.segmentIndex} - unique)')
        print('\t Time process work =', str(finish - start), '(sec)')
        print(' END \"Segmentation file\"...\n')
        return

if __name__ == '__main__':
    
    defaultConfigParameters = {
        'dataPath'          :'./../data/',
        'dataFileName'      :'9.txt',
        'binaryPath'        :'./../outputBinary/',
        'databaseFileName'  :'SegmentDB',
        'secretKeyFileName' :'SecretKey.dat',
        'segmentStorePrefix':'SegmentList',
        'segmentInOneFile'  :5000,
        'blockSize'         :4,
        'segmentInLine'     :2,
        'orderInLine'       :20,
        'separatorSegment'  :'|',
        'separatorOrder'    :'|'       
    }

    SegmentToFile(defaultConfigParameters).main()