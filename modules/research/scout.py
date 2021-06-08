import os, time, random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from striprtf.striprtf import rtf_to_text 

seconds = time.time()
local_time_1 = time.ctime(seconds)
print("Local time: ", local_time_1 , "\n")
start = time.time()

cd_path = os.path.abspath(r"C:/Users/skanyhin001/Desktop/Research tool/EDRSR/Court_decisions")
cd_path_list = os.listdir(cd_path)
random.shuffle(cd_path_list)
cd_path_list = cd_path_list[-10:]
cd_path_list_len = len(cd_path_list)
cd_count = 1
cd_empty_list = []
cd_df = pd.DataFrame(columns = ["cd","cd_par_count","cd_word_count"])

for cd in cd_path_list:
     
    try:
        cd_text = open(os.path.join(cd_path, cd), 'r').read()
        cd_text = rtf_to_text(cd_text).encode('cp1252')
        cd_text = cd_text.decode('cp1251')
        
        paragraphs = cd_text.splitlines()        
        cd_par_count = 0
        cd_word_count = 0
        for i in paragraphs:
            if not i=="":             
                cd_par_count += 1
                cd_par_text = i.split()
                cd_word_count += len(cd_par_text)
        if cd_word_count == 0: cd_empty_list.append(cd)   
        else:
            temp_df = pd.DataFrame({"cd":[cd],"cd_par_count":[cd_par_count],"cd_word_count":[cd_word_count]})
            cd_df = pd.concat([cd_df,temp_df])
    
    except Exception as e: 
        print(cd, "ERROR open", e)
        cd_text = open(os.path.join(cd_path, cd), 'r').read()
        print(cd_text)
        try:
            cd_text = rtf_to_text(cd_text).encode('cp1252')
            print(cd_text)
            cd_text = cd_text.decode('cp1251')
            print(cd_text)
        except Exception as e:
            print(e)
        
        #Cp437

    cd_count += 1
    if cd_count % 100 == 0: print("processed courts:", 
                                  cd_count, "of", 
                                  cd_path_list_len, 
                                  "(",int(100*cd_count/cd_path_list_len),
                                  "% )")   

seconds = time.time()
local_time_2 = time.ctime(seconds)
print("\n","Local time: ", local_time_2)
end = time.time()
print("Duration in seconds: ", int(end-start))
print("Duration in minutes: ", int((end-start)/60))