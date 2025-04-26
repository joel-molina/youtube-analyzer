### Pre-requisite
Hadoop installed on native Windows 11 device.
Python installed.

### Run Hadoop
In hadoop\sbin:  
start-dfs.cmd  
start-yarn.cmd

### Data upload
hdfs dfs -mkdir -p /user/youtube_data

hdfs fs -put [local_data_file_path] [hdfs_destination_path]

### Compile packages
Note: Dependencies are based on local filepath, adjust accordingly for actual local installation location of hadoop.

javac -classpath "C:\hadoop\etc\hadoop;C:\hadoop\share\hadoop\common\*;C:\hadoop\share\hadoop\common\lib\*;C:\hadoop\share\hadoop\hdfs\*;C:\hadoop\share\hadoop\hdfs\lib\*;C:\hadoop\share\hadoop\yarn\*;C:\hadoop\share\hadoop\yarn\lib\*;C:\hadoop\share\hadoop\mapreduce\*;C:\hadoop\share\hadoop\mapreduce\lib\*;C:\hadoop\lib\*" -d . com/mycompany/hadoop/*.java

### Make jar
job 1: jar -cvf DegreeDistribution.jar -C . com/mycompany/hadoop  
job 2: jar -cvf CategoryDistribution.jar -C . com/mycompany/hadoop

### Run jar
job 1: hadoop jar DegreeDistribution.jar com.mycompany.hadoop.DegreeDistribution /user/youtube_data/youtube_data_2008.tsv /user/output_temp  
job 2: hadoop jar CategoryDistribution.jar com.mycompany.hadoop.CategoryDistribution /user/youtube_data/youtube_data_2008.tsv /user/output_temp

### Download Output 
hdfs dfs -get /user/output_temp/part-r-00000


## Running python scripts
In the terminal go to the directory where the scripts are located and run:  
python degreeVis.py  
python categoryVis.py
