# PageRank_Wikipedia

Compiler and Platform

Compiler:     	Java compiler

Java version: 	JDK 1.7.0

Programming:   Java

Platform:     	Eclipse IDE

Compilation and execution in Terminal

Step to create folder in HDFS: Commands to create input folder in hdfs
1.	sudo su hdfs
2.	Hadoop fs -mkdir /user/cloudera
3.	Hadoop fs -chown cloudera /user/cloudera
4.	Exit
5.	Sudo su cloudera
6.	Hadoop fs -mkdir /user/cloudera/input

After Creating folder in HDFS add files to HDFS

Step 1: Place the input files in HDFS.

Command: hadoop fs -put < CanterburyFile folder Path> <Path in HDFS>

Example: Hadoop fs -put /home/cloudera/Downloads/cantrbry/Canterbury/ /user/cloudera/input/

Driver Class: It includes configurations to run the jobs based on the input and output it receives.

Job 1:

CalculateTotaLinksMapper: It parses the input XML document and checks if there is any valid data
between the <title> and </title> tags. The presence of valid data indicates that it is a valid title.
If such a vaild data is found then a key called "Count of Pages" is set and it's value is set to One.

CalculateTotalLinksReducer: It sums up all the values for the Key "Count of Pages" and sets value
to the count. The count indicates the number of valid pages between the titles tag in the XML data.

Job 2: 

CalculateInitialPageRankMapper: Configuration object is passed through this job. It has the titles count
obtained from the first job. In this class mapper is used to extract data from the XML file. It extracts 
the titles between the titles tags and sets it as the key. The spaces between the words in titles are replaced
by underscore. It also extracts data between the Text tags which has the outlinks for the corresponding titles.
The value for the key is the data between text tags. 

CalculateInitialPageRankReducer: Here initial pageRank is set to 1/N.
N: Number retrieved from the configuration object i.e title count from Job 1.
Here the key is the page title. The value is concatenation of pageRank value and list of outlinks
corresponding to a specific page title seperated by tab.

Job 3:

CalculateIterativePageRankMapper: The Driver class is set to run this specific job to iterate for ten times.
This is done so that the values converge. The output acts as input for the next iteration.
This is used to eliminate red links and the output is outlink and value is pagerank value and number of outlinks
for corresponding page title. 
It also outputs page title as key and value as outlinks list so that the procedure can be used for the next iteration.

CalculateIterativePageRankReducer: It calculates the pageRank using the pageRank formula and damping factor for a 
specific page title depending on the outlinks the page title has which is received from mapper. It eliminates the redlinks.
The output has the key and value similar to Job 2 but with updated pageRank value. The format is similar to Job 2 to 
make output understandable for the following iterations.

Job 4:

FinalSortedRankMapper: It retrieves the page title and page rank value obtained from iteration 10. The key is 
pageRank value and the value is page rank title.

FinalSortedRankReducer: It is used to sort the pageRank value in decreasing order by using the DescendingKeyComparator 
class which overrides the compare method. The output is the page title and the value is pageRank value in decreasing 
order of pageRank values. 

DescendingKeyComparator Class: It overrides the compare method and it is utilized by FinalSortedRankReducer 
to sort the pageRank values in the decreasing order.

Driver Class: It runs Jobs from one to four and later deletes all the folders in the output path except the
Sorted_PageRank folder which has the output file containing page titles and pageRanks sorted in decreasing order.

Execution Steps:

Step 1: Compile java file

Command: 
1.	mkdir -p build

2.	Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/wikipedia/org/Driver.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf Driver.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Output Folder Path in HDFS>

Example: Hadoop jar Driver.jar wikipedia.org.Driver /user/cloudera/input/ /user/cloudera/output/

To Run the program again Delete the output Folder or change the Output folder path during Execution.

Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: Hadoop fs -rm -r /home/cloudera/output/Sorted_PageRank

Sorted_PageRank is the folder containing output. It has page titles and pageRank values in decreasing order.

To re-execute the program delete the Sorted_PageRank folder and run it.
