# import pandas as pd
import numpy as np
import time

from pyspark.sql import SparkSession
# data visualization
import seaborn as sns

# machine learning
from sklearn import svm
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import StratifiedKFold
from sklearn import preprocessing
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score

# initialize some package settings
sns.set(style="whitegrid", color_codes=True, font_scale=1.3)

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
defConf = spark.sparkContext._conf.getAll()
conf = spark.sparkContext._conf.set('spark.cores.max', '16')
spark.sparkContext.stop()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

start = time.time()

f=open("/home/jrgil/eht_proj/2019-03-02_rec/results.txt","r")
lines=f.readlines()
features=[]
label=[]
for x in lines:    
    label.append(float(x.split(' ')[0]))
    features.append([int(x.split(' ')[1]), int(x.split(' ')[2])])
f.close()


features=np.array(features)

#transform labels

le=LabelEncoder()
label=le.fit_transform(label)

#split! antes de 'entrenar' el tfidfizer

skf = StratifiedKFold(n_splits=5,random_state=0)
for train_index, test_index in skf.split(features, label):
    break

featuresTrain=features[train_index]
featuresTest=features[test_index]
labelTrain=label[train_index]
labelTest=label[test_index]


clf = LinearSVC(loss='hinge', class_weight='balanced')
clf.fit(featuresTrain, labelTrain)

pred=clf.predict(featuresTest)


print("**************************************Accuracy of predictions:")
print(accuracy_score(labelTest,pred))
end = time.time()
print("****************************************************TIME: " + str(end - start))