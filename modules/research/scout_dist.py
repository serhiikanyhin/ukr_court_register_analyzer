import os, time, random, re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from striprtf.striprtf import rtf_to_text 

# TIME

seconds = time.time()
local_time_1 = time.ctime(seconds)
print("Local time: ", local_time_1 , "\n")
start = time.time()

# PARAMETRES

cd_path = os.path.abspath(r"C:/Users/skanyhin001/Desktop/Research tool/EDRSR/Court_decisions")
cd_path_list = os.listdir(cd_path)
random.shuffle(cd_path_list)
#cd_path_list = cd_path_list[-100:]
cd_path_list_len = len(cd_path_list)
cd_count = 1
cd_empty_list = []
cd_except_list = []
cd_df = pd.DataFrame(columns = ["cd","cd_par_count","cd_word_count"])

# FILES PROCESSING

for cd in cd_path_list: 
    try:
        cd_text = open(os.path.join(cd_path, cd), 'r').read()
        cd_text = rtf_to_text(cd_text).encode('cp1252', 'ignore').decode('cp1251')
        
        for x in ["\\x00","\\xa0"]: cd_text = re.sub(x, "", cd_text)
        for x in [" ","\n","\r"]:   cd_text = re.sub(x+"+", x, cd_text)
        for x in [" \n","\n "]:     cd_text = re.sub(x, "\n", cd_text)
            
        paragraphs = [i for i in cd_text.splitlines() if i != ""]
        if paragraphs == []: cd_empty_list.append(cd)
        else:
            cd_par_count = 0
            cd_sentense_count = 0
            cd_word_count = 0
            for i in paragraphs:
                cd_par_count += 1
                cd_word_count += len(i.split())
                cd_sentense_count += len(i.split(". "))    
            cd_df = pd.concat([cd_df,pd.DataFrame({"cd":[cd],
                                        "cd_par_count":[cd_par_count],
                                        "cd_sentense_count":[cd_sentense_count],
                                        "cd_word_count":[cd_word_count]})])
    except Exception as e: cd_except_list.append(cd)
    
    # PROCESSING PROGRESS

    cd_count += 1
    if cd_count % 100 == 0: print("processed courts:", 
                                  cd_count, "of", 
                                  cd_path_list_len, 
                                  "(",int(100*cd_count/cd_path_list_len),
                                  "% )")   

# RESULTS OF PROCESSING

print("\nnumber of except cd:", len(cd_except_list))#,"\nlist:",cd_except_list)
print("\nnumber of empty cd:", len(cd_empty_list))#,"\nlist:",cd_empty_list)
cd_df = cd_df.sort_values(by=['cd_word_count'], ascending=True)
cd_df.index = range(len(cd_df))
#print("\ndataframe:\n", cd_df)
cd_df.to_csv(r"C:/Users/skanyhin001/Desktop/Research tool/Scouting/cd_df.csv", index = True)

# CLEAR DATAFRAME

cd_clear_df = cd_df
cd_clear_df = cd_clear_df[cd_clear_df['cd_par_count'] <= 400]
cd_clear_df = cd_clear_df[cd_clear_df['cd_sentense_count'] <= 500]
cd_clear_df = cd_clear_df[cd_clear_df['cd_word_count'] <= 15000]
#print("\nclear dataframe:\n", cd_clear_df)
cd_clear_df.to_csv(r"C:/Users/skanyhin001/Desktop/Research tool/Scouting/cd_clear_df.csv", index = True)

# DISTRIBUTION PLOTS

fig,axes=plt.subplots(1,3)
fig.set_size_inches(25, 10)
sns.distplot(cd_clear_df['cd_par_count'],ax=axes[0], bins=50)
sns.distplot(cd_clear_df['cd_sentense_count'],ax=axes[1], bins=50)
sns.distplot(cd_clear_df['cd_word_count'],ax=axes[2], bins=50)
axes[0].set_ylabel('frequency')
axes[0].set_xlabel('paragraph count')
axes[1].set_xlabel('sentense count')
axes[2].set_xlabel('word count')
for x in [0,1,2]:
    axes[x].spines["top"].set_visible(False)
    axes[x].spines["right"].set_visible(False)
    axes[x].spines["left"].set_visible(False)
for x in [1,2]:
    axes[x].yaxis.set_label_position("right")
    axes[x].yaxis.tick_right()
axes[0].set_xlim([0, 400])
axes[1].set_xlim([0, 500])
axes[2].set_xlim([0, 15000])
plt.show()

# CHUNKS

fig,axes=plt.subplots(1,3)
fig.set_size_inches(25, 10)

for l, x in enumerate(['cd_par_count','cd_sentense_count','cd_word_count']):

    split_cd_df = np.array_split(cd_clear_df, 10)
    chunk_df = pd.DataFrame(columns = ["chunk number","min","mean","max"])
    for idx, chunk in enumerate(split_cd_df):
        df = pd.DataFrame(chunk)
        chunk_min = df[x].min()
        chunk_mean = df[x].mean()
        chunk_max = df[x].max()
        chunk_df = pd.concat([chunk_df, pd.DataFrame({"chunk number":[idx+1],
                                  "min":[chunk_min],
                                  "mean":[chunk_mean],
                                  "max":[chunk_max]})])
        #print(chunk_df)
        chunk_df.to_csv(r"C:/Users/skanyhin001/Desktop/Research tool/Scouting/chunk_df_"+x+".csv", index = False)
    sns.barplot(x="chunk number", y="mean", data=chunk_df,ax=axes[l], palette="Blues")
    axes[l].set_ylabel("chunk number")
    axes[l].set_xlabel(x)
    for i in ["top","right","left"]: axes[l].spines[i].set_visible(False)
    
# TIME

seconds = time.time()
local_time_2 = time.ctime(seconds)
print("\nLocal time: ", local_time_2)
end = time.time()
print("Duration in seconds: ", int(end-start))
print("Duration in minutes: ", int((end-start)/60))