
part1:

1.download list from http://www.cs.uic.edu/~liub/FBS/opinion-lexicon-English.rar
2. unzip it, copy opinion-lexicon-Englishhdfs folder to SSH then upload it to HDFS

hdfs dfs -copyFromLocal opinion-lexicon-English /user/yxl160531

3. copy your jar file to SSH

4. make a directory /user/yxl160531/Assignment1/bi/ and copy text files that generated in Assignment1 part1

5. run jar file to analysis 6 txt files in bi
hadoop jar cs6350_assn1b-0.0.1-SNAPSHOT.jar cs6350.assn_1b.WordCount  /user/yxl160531/Assignment1/bi/*.txt /user/yxl160531/Assignment1/bi/result_bi

6. then view the result in result_bi folder 

hdfs dfs -cat /user/yxl160531/Assignment1/bi/result_bi/part-r-00000

The output is:
neg: 20166
pos: 22620

part2:
1. Download POS list from For http://icon.shef.ac.uk/Moby/mpos.html, then you get mpos.tar.Z

2. upload mpos.tar.Z to SSH and unzip it, then you get a folder named mpos

3. make a directory /user/yxl160531/Assignment1/bii and copy text file that genenrated in Assignment part2


4. run jar file to analyis the file in bii 
hadoop jar cs6350_assn1b-0.0.1-SNAPSHOT.jar cs6350.assn_1b.PosCount /user/yxl160531/Assignment1/bii/text.txt /user/yxl160531/Assignment1/bii/result_bii

5. view the result in result_bii folder
hdfs dfs -cat /user/yxl160531/Assignment1/bii/result_bii/part-r-00000


