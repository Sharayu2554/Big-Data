/***************** Execute Following Commands in sequential Order  *****************************/
/**************** go to HW1 folder and run the following commands ******************************/

hadoop dfs -mkdir /submission
hadoop dfs -mkdir /submission/BigDataAssignment1

______________________Q1 Commands_______________________

hadoop dfs -mkdir /submission/BigDataAssignment1/Q1MutualFriend
hadoop dfs -mkdir /submission/BigDataAssignment1/Q1MutualFriend/input
hadoop dfs -mkdir /submission/BigDataAssignment1/Q1MutualFriend/output

hadoop dfs -put ./Q1/input.txt /submission/BigDataAssignment1/Q1MutualFriend/input/

hadoop  dfs  -rm /submission/BigDataAssignment1/Q1MutualFriend/output/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q1MutualFriend/output

hadoop jar ./Q1/MutualFriend.jar  Q1MutualFriend.MutualFriend /submission/BigDataAssignment1/Q1MutualFriend/input/input.txt /submission/BigDataAssignment1/Q1MutualFriend/output

rm ./Q1/Output/*

hadoop dfs -ls /submission/BigDataAssignment1/Q1MutualFriend/output

hadoop dfs -get /submission/BigDataAssignment1/Q1MutualFriend/output/*  ./Q1/Output/

/** Output is in ./Q1/Output/ folder **/

______________________Q2 Commands_______________________

hadoop dfs -mkdir /submission/BigDataAssignment1/Q2TopMutualFriend
hadoop dfs -mkdir /submission/BigDataAssignment1/Q2TopMutualFriend/input
hadoop dfs -mkdir /submission/BigDataAssignment1/Q2TopMutualFriend/output 

hadoop dfs -put ./Q2/input.txt /submission/BigDataAssignment1/Q2TopMutualFriend/input


hadoop  dfs  -rm /submission/BigDataAssignment1/Q2TopMutualFriend/output/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q2TopMutualFriend/output

hadoop  dfs  -rm /submission/BigDataAssignment1/Q2TopMutualFriend/output_temp/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q2TopMutualFriend/output_temp

hadoop jar ./Q2/TopMutualFriend.jar  Q2TopMutualFriend.TopMutualFriend /submission/BigDataAssignment1/Q2TopMutualFriend/input/input.txt /submission/BigDataAssignment1/Q2TopMutualFriend/output_temp /submission/BigDataAssignment1/Q2TopMutualFriend/output

rm ./Q2/Output/*
rm ./Q2/Output_temp/*

hadoop dfs -get /submission/BigDataAssignment1/Q2TopMutualFriend/output_temp/*   ./Q2/Output_temp
hadoop dfs -get /submission/BigDataAssignment1/Q2TopMutualFriend/output/*   ./Q2/Output

/** Output is in ./Q2/Output/ folder **/


_____________________Q3 Commands__________________________

hadoop dfs -mkdir /submission/BigDataAssignment1/Q3MeanVariance
hadoop dfs -mkdir /submission/BigDataAssignment1/Q3MeanVariance/input
hadoop dfs -mkdir /submission/BigDataAssignment1/Q3MeanVariance/output 

hadoop dfs -put ./Q3/numbers.txt /submission/BigDataAssignment1/Q3MeanVariance/input


hadoop  dfs  -rm /submission/BigDataAssignment1/Q3MeanVariance/output/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q3MeanVariance/output

hadoop jar ./Q3/MeanVarianceMR.jar  Q3MeanVariance.MeanVarianceMR /submission/BigDataAssignment1/Q3MeanVariance/input/numbers.txt /submission/BigDataAssignment1/Q3MeanVariance/output

rm ./Q3/Output/*

hadoop dfs -get /submission/BigDataAssignment1/Q3MeanVariance/output/*   ./Q3/Output

/** Output is in ./Q3/Output/ folder **/

_________________Q4 Commands ____________________________________


hadoop dfs -mkdir /submission/BigDataAssignment1/Q4MinMaxMedian
hadoop dfs -mkdir /submission/BigDataAssignment1/Q4MinMaxMedian/input
hadoop dfs -mkdir /submission/BigDataAssignment1/Q4MinMaxMedian/output 

hadoop dfs -put ./Q4/numbers.txt /submission/BigDataAssignment1/Q4MinMaxMedian/input

hadoop  dfs  -rm /submission/BigDataAssignment1/Q4MinMaxMedian/output/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q4MinMaxMedian/output

hadoop jar ./Q4/MinMaxMedian.jar  Q4MinMaxMedian.MinMaxMedian /submission/BigDataAssignment1/Q4MinMaxMedian/input/numbers.txt /submission/BigDataAssignment1/Q4MinMaxMedian/output

rm ./Q4/Output/*

hadoop dfs -get /submission/BigDataAssignment1/Q4MinMaxMedian/output/*   ./Q4/Output

/** Output is in ./Q4/Output/ folder **/

______________Q5 Commands ___________________________________________

hadoop dfs -mkdir /submission/BigDataAssignment1/Q5MatrixMultiplication
hadoop dfs -mkdir /submission/BigDataAssignment1/Q5MatrixMultiplication/input
hadoop dfs -mkdir /submission/BigDataAssignment1/Q5MatrixMultiplication/output

hadoop dfs -put ./Q5/p5-input.txt  /submission/BigDataAssignment1/Q5MatrixMultiplication/input
hadoop dfs -put ./Q5/input_test.txt  /submission/BigDataAssignment1/Q5MatrixMultiplication/input

hadoop  dfs  -rm -r /submission/BigDataAssignment1/Q5MatrixMultiplication/output_tmp/*
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q5MatrixMultiplication/output_tmp

hadoop  dfs  -rm -r /submission/BigDataAssignment1/Q5MatrixMultiplication/output/*#
hadoop  dfs  -rmdir  /submission/BigDataAssignment1/Q5MatrixMultiplication/output

#hadoop jar ./Q5/MatrixMultiplicationJobChaining.jar  Q5MatrixMultiplication.MatrixMultiplicationJobChaining /sub#mission/BigDataAssignment1/Q5MatrixMultiplication/input/p5-input.txt /submission/BigDataAssignment1/Q5MatrixMultiplication/output_tmp /submission/BigDataAssignment1/Q5MatrixMultiplication/output

hadoop jar ./Q5/MatrixMultiplicationJobChaining.jar  Q5MatrixMultiplication.MatrixMultiplicationJobChaining /submission/BigDataAssignment1/Q5MatrixMultiplication/input/input_test.txt /submission/BigDataAssignment1/Q5MatrixMultiplication/output_tmp /submission/BigDataAssignment1/Q5MatrixMultiplication/output

rm ./Q5/OutputDemo/*

hadoop dfs -get /submission/BigDataAssignment1/Q5MatrixMultiplication/output/*   ./Q5/OutputDemo

/** Output is in ./Q5/Output/ folder **/
/** Output Of this is uploaded on drive , it is 3.5 GB Aprox **
/** sharable link is given in comment **/
_______________________________________________________________________


