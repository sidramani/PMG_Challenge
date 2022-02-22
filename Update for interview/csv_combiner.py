import sys
import os
from pathlib import Path
import pandas as pd

def combine_csv(arg):

    #checks if at least two input arguments are provided
    if len(arg) < 2:
        print ("Not enough arguments provided, need at least 2 input files")
    
    #creates a list of distinct input files to avoid duplication and preserve order    
    files = list(dict.fromkeys(arg))
    
    #combined_csv = pd.DataFrame()
    chunksize = 100000
    
    for i in range(len(files)):
        #checks if filepath exists
        if os.path.exists(files[i]) != True:
            raise IOError (os.path.basename(files[i]) + " is not found in path")
        
        #checks if input files are in correct format
        if Path(files[i]).suffix != '.csv':
            raise Exception (Path(files[i]).suffix + ' is invalid. Input must be in .csv format')
        
        filename = os.path.basename(files[i])
        
        #reads every file in the loop in predefined chunks
        for chunk in pd.read_csv(files[i], chunksize=chunksize):
            
            chunk['filename'] = filename
            #outputs chunks in .csv format
            print (chunk.to_csv(index = False, line_terminator='\n', header = True if i == 0 else False), end = '')
    
    
if __name__ == '__main__':
    combine_csv(sys.argv[1:])