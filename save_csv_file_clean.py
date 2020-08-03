import os 

def _save_data(namefile, data): 
    ''' method for save the  data with no missing value in a csv file'''
    filename =os.path.split(namefile)
    newFileName = filename[0]+'/'+filename[1].split('.')[0]+"(no_missing)_"+".csv"
    
    print(data.columns)
    with open(newFileName,  mode='w+', encoding='utf-8') as f: 
        data.to_csv(f, sep="|", index=False)