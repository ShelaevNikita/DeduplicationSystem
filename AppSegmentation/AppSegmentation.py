
import ToSegment, FromSegment

class mainClassSegment():
    
    def __init__(self):
        self.defaultConfigFile = './configFile.txt'      
        self.configParameters  = {}
    
    def inputConfigFile(self):
        inputFlag = input(f'\t Use default config file (\"{self.defaultConfigFile}\")? (Y / N): ')
        if inputFlag.strip().lower().startswith('y'):
            return
        newConfigFile = input('\t Enter path and name your config file: ')
        self.defaultConfigFile = newConfigFile.strip()
        return

    def splitConfigFile(self):
        dataParameters = ''
        try:
            with open(self.defaultConfigFile, 'r') as fileData:
                dataParameters = fileData.readlines()
        except FileNotFoundError:
            print('\t ERROR: Not found config file...')
            return
        for line in dataParameters:
            lineParameter  = line.replace('\n', ' ').replace(' ', '')
            splitParameter = lineParameter.split('=')
            parameterName  = splitParameter[0]
            parameterValue = splitParameter[1]
            try:
                parameterValue = int(parameterValue)
            except ValueError:
                pass
            self.configParameters[parameterName] = parameterValue
        return
    
    def orderWork(self):
        segmentToFileFlag = input('\t Do \"Segmentation file\"? (Y / N): ')
        if segmentToFileFlag.strip().lower().startswith('y'):
            ToSegment.SegmentToFile(self.configParameters).main()
        fileFromSegmentFlag = input('\t Do \"Collect segments into file\"? (Y / N): ')
        if fileFromSegmentFlag.strip().lower().startswith('y'):
            FromSegment.FileFromSegment(self.configParameters).main()
        return

    def main(self):
        print(' Hello!')
        self.inputConfigFile()
        self.splitConfigFile()
        self.orderWork()
        return

if __name__ == '__main__':
    mainClassSegment().main()