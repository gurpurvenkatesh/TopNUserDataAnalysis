# TomNUserDataAnalysis

1) Problem Statement : hadoop program to find out the top N users.
2) Map Input {key, value} pair : { byte offset, User xml record} : {IntWritable, Text} 
3) Map Output {key, value} pair : {NULL, <userName, Reputation Score>} : {NullWritable, Text} 
4) Mapper logic 
        Get the 'N' : How many in the top? Top 3, Top 5? 
        Parse XML to get the userId & the reputation score. 
        Write the code to check if the reputation score is in Top N 
        Use Java Tree Map where data are sorted by keys. 
        Use the reputation score as Key 
        When the tree size increases beyond Top N then remove the latest data from the Tree Map which is basically lowest record. 
        Write Map Output {key, value} pairs. 
5) Reducer Input {key, value} pair : {NULL, <userName, Reputation Score>} : {NullWritable, Text} 
6) Reducer Outpit {jey, value} pair : {Username, Reputation Score} : {Text, IntWritable} 
7) Reducer Logic 
      Get the 'N' : How many in the top? Top 3, Top 5? 
      Iterate through the Mapper outputs. Key for each value is same which is Null. 
      Get the Reputation score & the userName using Map Output. 
      If the score is in Top N then retain it or reject it 
      Write the output into HDFS.
