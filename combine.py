import sys
import os
from pathlib import Path
import dask.dataframe as dd

def readCSV(file):
    #uses a dask dataframe to read CSV files
    df = dd.read_csv(file)
    
    #creates column for associated filename and specifying datatype
    df['filename'] = os.path.basename(file)
    df['filename'] = df['filename'].astype(str)
    
    return df

def combineCSV(arg):
    
    #checks if at least two input arguments are provided
    if len(arg[:-1]) < 2:
        print ("Not enough arguments provided, need at least 2 input files")
    
    #creates a list of distinct input files to avoid duplication and preserve order    
    files = list(dict.fromkeys(arg[:-1]))
    
    for i in files:
        #checks if filepath exists
        if os.path.exists(i) != True:
            raise IOError (os.path.basename(i) + " is not found in path")
        
        #checks if input files are in correct format
        if Path(i).suffix != '.csv':
            raise Exception (Path(i).suffix + ' is invalid. Input must be in .csv format')
    
    #reades input files by calling readCSV() function        
    fileList = [readCSV(file) for file in files]
    
    #concatenates all files along rows and saves into a master CSV file (last argument)
    combined_csv = dd.concat(fileList, axis = 0)
    combined_csv.to_csv(arg[-1], single_file = True, index = False)
    
    
if __name__ == '__main__':
    combineCSV(sys.argv[1:])

