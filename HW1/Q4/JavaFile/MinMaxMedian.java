package Q4MinMaxMedian;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.log4j.Logger;

public class MinMaxMedian {
	
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		//private Logger logger = Logger.getLogger(Map.class);
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			context.write(new LongWritable(1L), new LongWritable(Long.parseLong(value.toString())));
		}

	}
	
	public static class Reduce extends Reducer<LongWritable,LongWritable,Text,Text> { 
		//private Logger logger = Logger.getLogger(Reduce.class);
		private ArrayList<Long> valueList = new ArrayList<Long>();
		
		public void reduce(LongWritable key, Iterable<LongWritable> values,Context context) 
				throws IOException, InterruptedException 
		{
			Long median = 0L;
			int count = 0;
			Long min = Long.MIN_VALUE;
			Long max = Long.MAX_VALUE;
			
			valueList.clear();
			//result.setStdDev(0);
			
			// Iterate through all input values for this key
			for (LongWritable val : values) {
				valueList.add((Long) val.get());
				++count;
			}

			Collections.sort(valueList);
			if (count % 2 == 0) {
				median = (valueList.get((int)count/2 -1) + valueList.get((int)count/2))/2;
				
			} else {
				// else, set median to middle value
				median = valueList.get((int)count/2);
			}
			
			min = valueList.get(0);
			max = valueList.get(valueList.size() -1);
			
			context.write(new Text(min.toString()), new Text(max.toString() + "\t"+ median.toString()));
		}
	}


  	// Driver program
  	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
		  	System.err.println("Usage: MinMaxMedian <in> <out>");
		  	System.exit(2);
	  	}
	  	
	  	@SuppressWarnings("deprecation")
	  	Job job = new Job(conf, "MinMaxMedian");
	  	job.setJarByClass(MinMaxMedian.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setNumReduceTasks(1);
	  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	  	// set output key type
	  	
	  	job.setMapOutputKeyClass(LongWritable.class);
	  	job.setMapOutputValueClass(LongWritable.class);
	  	
	  	job.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job.setOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	//Wait till job completion
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
