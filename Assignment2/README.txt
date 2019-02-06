Import the Java project into eclipse and build up assn2.jar and upload it to cs6360.utdallas.edu cluster.

After that, type in the following command to execute code for Q1-Q5 and see the corresponding results:

Q1:
hadoop jar assn2.jar cs6350.assn2.yelp.Q1 /yelp/business/business.csv /user/yxl160531/Assignment2/Q1
hdfs dfs -cat /user/yxl160531/Assignment2/Q1/part-r-00000

Q2:
hadoop jar assn2.jar cs6350.assn2.yelp.Q2 /yelp/business/business.csv /user/yxl160531/Assignment2/Q2
hdfs dfs -cat /user/yxl160531/Assignment2/Q2/part-r-00000

Q3:
hadoop jar assn2.jar cs6350.assn2.yelp.Q3 /yelp/business/business.csv /user/yxl160531/Assignment2/Q3
hdfs dfs -cat /user/yxl160531/Assignment2/Q3/part-r-00000

Q4:
hadoop jar assn2.jar cs6350.assn2.yelp.Q4 /yelp/review/review.csv /user/yxl160531/Assignment2/Q4
hdfs dfs -cat /user/yxl160531/Assignment2/Q4/part-r-00000

Q5:
hadoop jar assn2.jar cs6350.assn2.yelp.Q5 /yelp/review/review.csv /user/yxl160531/Assignment2/Q5
hdfs dfs -cat /user/yxl160531/Assignment2/Q5/part-r-00000