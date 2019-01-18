package Q3MeanVariance;

import Q3MeanVariance.MapperOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;

public class MeanVarianceMR {
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, MapperOutput>{
		private Logger logger = Logger.getLogger(Map.class);
		
		private DoubleWritable sumOfNo = new DoubleWritable();
		private IntWritable countOfNo = new IntWritable();
		private DoubleWritable sumSquareOfNo = new DoubleWritable();
		
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			//logger.info("value " + Double.parseDouble(value.toString()));
			Double dvalue = Double.parseDouble(value.toString());
			sumOfNo.set(sumOfNo.get() + dvalue);
			countOfNo.set(countOfNo.get() + 1);
			sumSquareOfNo.set(sumSquareOfNo.get() + (dvalue * dvalue));
			
			logger.info("sumOfNo : " + sumOfNo);
			logger.info("countOfNo : " + countOfNo);
			logger.info("sumSquareOfNo : " + sumSquareOfNo);
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			MapperOutput value = new MapperOutput(sumOfNo, countOfNo, sumSquareOfNo);
			logger.info("Mapper output value " + value);
			context.write(new LongWritable(1),value );
		}

	}
	
	public static class Reduce extends Reducer<LongWritable,MapperOutput,DoubleWritable,DoubleWritable> { 
		private Logger logger = Logger.getLogger(Reduce.class);
		
		public void reduce(LongWritable key, Iterable<MapperOutput> values,Context context) 
				throws IOException, InterruptedException 
		{
			logger.info("reducer values " + values);
			DoubleWritable totalSumOfNo = new DoubleWritable(0);
			IntWritable totalCountOfNo = new IntWritable(0);
			DoubleWritable totalSumSquareOfNo = new DoubleWritable(0);
			
			for(MapperOutput value : values) 
			{
				//logger.info("value : " + value);
				totalSumOfNo.set(totalSumOfNo.get() + value.getSumOfNo());
				totalCountOfNo.set(totalCountOfNo.get() + value.getCountOfNo());
				totalSumSquareOfNo.set(totalSumSquareOfNo.get() + value.getSumSquareOfNo());
			}
			
			double mean = totalSumOfNo.get()/totalCountOfNo.get();
			double X = (totalSumOfNo.get() * totalSumOfNo.get())/totalCountOfNo.get();
			double Y = totalSumSquareOfNo.get() - X;
			double SD = Math.sqrt(Y/(totalCountOfNo.get() -1));
			double variance = (SD * SD)/totalCountOfNo.get();
			context.write(new DoubleWritable(mean),new DoubleWritable(variance));
			// create a pair <keyword, number of occurences>
		}
	}


  	// Driver program
  	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
		  	System.err.println("Usage: MeanVarianceMR <in> <out>");
		  	System.exit(2);
	  	}
	  	// create a job with name "MututalFriend"
	  	@SuppressWarnings("deprecation")
	  	Job job = new Job(conf, "MeanVarianceMR");
	  	job.setJarByClass(MeanVarianceMR.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setNumReduceTasks(1);
	  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	  	// set output key type
	  	
	  	job.setMapOutputKeyClass(LongWritable.class);
	  	job.setMapOutputValueClass(MapperOutput.class);
	  	
	  	job.setOutputKeyClass(DoubleWritable.class);
	  	// set output value type
	  	job.setOutputValueClass(DoubleWritable.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	//Wait till job completion
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
  }
