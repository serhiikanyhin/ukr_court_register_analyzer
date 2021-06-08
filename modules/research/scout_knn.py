import time, sklearn
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from mlxtend.plotting import plot_decision_regions

from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB 
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

# TIME

seconds = time.time()
local_time_1 = time.ctime(seconds)
print("Local time: ", local_time_1 , "\n")
start = time.time()

# DARA PREPARATION

knn_df = pd.read_csv(r"C:/Users/skanyhin001/Desktop/Research tool/Scouting/cd_clear_df.csv")
#knn_df = pd.read_csv(r"C:/Users/skanyhin001/Desktop/Research tool/Scouting/cd_df.csv")
knn_df = knn_df.rename(columns={"Unnamed: 0": "row_id"})

def conditions(s):
    x = len(knn_df.index)/4
    if s["row_id"] < x: return 1
    if s["row_id"] < x*2 and s["row_id"] >= x: return 2
    if s["row_id"] < x*3 and s["row_id"] >= x*2: return 3
    else: return 4

knn_df['group_id'] = knn_df.apply(conditions, axis=1)

#Examining the files

X = knn_df[['cd_par_count', 'cd_word_count', 'cd_sentense_count']]
Y = knn_df['group_id']
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, Y, random_state=0)

cmap = plt.cm.get_cmap('gnuplot')
scatter = pd.plotting.scatter_matrix(X_train, c = y_train, marker = 'o', s=40, hist_kwds={'bins':45}, figsize=(9,9), cmap=cmap)

# plotting a 3D scatter plot

fig = plt.figure()
ax = fig.add_subplot(111, projection = '3d')
ax.scatter(X_train['cd_par_count'], X_train['cd_word_count'], X_train['cd_sentense_count'], c = y_train, marker = 'o', s=100)
ax.set_xlabel('cd_par_count')
ax.set_ylabel('cd_word_count')
ax.set_zlabel('cd_sentense_count')
plt.show()

# TRAIN-TEST SPLIT

# For this example, we use the mass, width, and height features of each fruit instance
X = knn_df[['cd_par_count', 'cd_word_count', 'cd_sentense_count']]
Y = knn_df['group_id']

# default is 75% / 25% train-test split
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, Y, random_state=0)

#CLASSIFIER OBJECT
knn = KNeighborsClassifier(n_neighbors = 5)

#Train the classifier (fit the estimator) using the training files
knn.fit(X_train, y_train)

#Estimate the accuracy of the classifier on future files, using the test files
knn_score = knn.score(X_test, y_test)
print("knn_score", knn_score)

#Use the trained k-NN classifier model to classify new, previously unseen objects
# example:
example_prediction = knn.predict([[100, 100, 100]])
print("example_prediction", example_prediction)

# Plot the decision boundaries of the k-NN classifier

def knn_comparison(k, x, y):
    
    # в "х" должно быть две колонки, в "у" - одна (label)
    x = x.to_numpy()
    y = y.to_numpy()
    clf = KNeighborsClassifier(n_neighbors=k)
    clf.fit(x, y)
    
    # Plotting decision region
    plot_decision_regions(x, y, clf=clf, legend=2)
    # Adding axes annotations
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Knn with K='+ str(k)+"(SVC)")
    plt.show()

knn_comparison(5, knn_df[['cd_par_count', 'cd_word_count']],knn_df['group_id'])





# How sensitive is k-NN classification accuracy to the choice of the 'k' parameter?

k_range = range(1,20)
scores = []
for k in k_range:    
    knn = KNeighborsClassifier(n_neighbors = k)
    knn.fit(X_train, y_train)
    scores.append(knn.score(X_test, y_test))
plt.figure()
plt.xlabel('k')
plt.ylabel('accuracy')
plt.scatter(k_range, scores)
plt.ylim([0, 1])

#How sensitive is k-NN classification accuracy to the train/test split proportion?

#t = [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2]
#knn = KNeighborsClassifier(n_neighbors = 5)
#plt.figure()
#for s in t:
#    scores = []
#    for i in range(1,1000):
#        X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, Y, test_size = 1-s)
#        knn.fit(X_train, y_train)
#        scores.append(knn.score(X_test, y_test))
#    plt.plot(s, np.mean(scores), 'bo')
#plt.xlabel('Training set proportion (%)')
#plt.ylabel('accuracy')
#plt.ylim([0, 1])

# TIME

seconds = time.time()
local_time_2 = time.ctime(seconds)
print("\nLocal time: ", local_time_2)
end = time.time()
print("Duration in seconds: ", int(end-start))
print("Duration in minutes: ", int((end-start)/60))