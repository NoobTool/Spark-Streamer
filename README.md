## AIM
I created a spark streaming program with Scala Maven to monitor a folder in HDFS in real time such that any
new file presented in the folder will be processed using regular expressions to remove special characters. Afterwards, the word frequency will be calculated followed by the removal of short words.
The co-occurence frequency (the words are considered co-occurred if they are in the same line) of each word will be computed using two methods:-

1. Using general methods like initializing and incrementing a count variable. 
2. Using updateStateByKey method and continuously updating the co-occurence frequency. 

## INSTRUCTIONS

The command to run the jar file to produce the desired outputs are is:-

spark-submit --class streaming.NetworkWordCount --master yarn --deploy-mode client Scala.jar <path-to-folder-to-be-monitored> <path-of-directory-where-output-must-be-stored>/

#### Note:- A forward slash must be put after the "path-of-directory-where-output-must-be-stored"

An example command is :-
spark-submit --class streaming.NetworkWordCount --master yarn --deploy-mode client Scala.jar /user/s3853868/input /user/s3853868/output/

## Understanding the output

The output folder will consist of 3 more subfolders named as task1, task2 and task3 which will contain the output of corresponding tasks. Also, the outputs of the respective tasks are saved separately in the folder with name as the current date and timestamp.


## Other information

Folder: ./Scala
Contains the main project folder used to complete the above aim. Created in Eclipse IDE with 'Maven Integration for Eclipse'. It can be downloaded from eclipse marketplace. 

Folder: ./Scala.jar
It represents the jar file to be executed.

