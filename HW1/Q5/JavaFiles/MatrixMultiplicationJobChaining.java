package Q5MatrixMultiplication;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MatrixMultiplicationJobChaining {

	//public static final int AK = 10000;
	//public static final int BK = 1000;
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		//private Logger logger = Logger.getLogger(Map.class);
			
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			String input = value.toString();
			String[] values = input.split(",");
			if(values[0].equals("A")) {
				context.write(new Text(values[2]), value);
			}
			else {
				context.write(new Text(values[1]), value);
			}
		}

	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> { 
		//private Logger logger = Logger.getLogger(Reduce.class);
		
		public void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException 
		{
			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>();
			HashMap<Integer, Double> hashB = new HashMap<Integer, Double>();
			
			for(Text val : values) {
				String[] value = val.toString().split(",");
				if(value[0].equals("A")) {
					hashA.put(Integer.parseInt(value[1].replaceAll(" ", "")), Double.parseDouble(value[3].replaceAll(" ", "")));
				}
				else {
					hashB.put(Integer.parseInt(value[2].replaceAll(" ", "")), Double.parseDouble(value[3].replaceAll(" ", "")));
				}
			}
			
			for(Integer i : hashA.keySet()) {
				for(Integer j : hashB.keySet()) {
					Double result = hashA.get(i) * hashB.get(j);
					context.write(null, new Text(i + "," + j +  "," + result.toString()));
				}
			}
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		//private Logger logger = Logger.getLogger(Map2.class);
			
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			String[] values = value.toString().split(",");
			Text key1 = new Text(values[0]+","+values[1]);
			context.write(key1, new Text(values[2]));
		}

	}
	
	public static class Reduce2 extends Reducer<Text,Text,Text,Text> { 
		//private Logger logger = Logger.getLogger(Reduce2.class);
		
		public void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException 
		{
			Double sum = new Double(0);
			for(Text value : values) {
				sum = sum + Double.parseDouble(value.toString());
			}
			context.write(new Text("X," + key.toString()), new Text(sum.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		/**************************JOB 1***********************/
		Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	
	  	if (otherArgs.length != 3) {
		  	System.err.println("Usage: MatrixMultiplicationJobChaining <in> <temp_output> <out>");
		  	System.exit(2);
	  	}
	  	
		@SuppressWarnings("deprecation")
	  	Job job = new Job(conf, "MatrixMultiplicationJobChaining");
	  	job.setJarByClass(MatrixMultiplicationJobChaining.class);
	  	
	  	Path in = new Path(otherArgs[0]);
		Path out = new Path(otherArgs[1]);
		
	  	FileInputFormat.addInputPath(job, in);
	  	FileOutputFormat.setOutputPath(job, out);
	  	
	  	job.setNumReduceTasks(2);
	  	
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	
	  	job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
	  	job.setMapOutputKeyClass(Text.class);
	  	job.setMapOutputValueClass(Text.class);
	  	
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	
	  	job.waitForCompletion(true);
	  	
	  	/**************************JOB 2**********************/
	  	
	  	Configuration conf2 = new Configuration();
	  	
	  	@SuppressWarnings("deprecation")
	  	Job job2 = new Job(conf2, "MatrixMultiplicationJobChainingJoin");
	  	job2.setJarByClass(MatrixMultiplicationJobChaining.class);
	  	
		Path out2 = new Path(otherArgs[2]);
		
	  	FileInputFormat.addInputPath(job2, out);
	  	FileOutputFormat.setOutputPath(job2, out2);
	  	
	  	//job.setNumReduceTasks(2);
	  	
	  	job2.setMapperClass(Map2.class);
	  	job2.setReducerClass(Reduce2.class);
	  	
	  	//job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
	  	job2.setMapOutputKeyClass(Text.class);
	  	job2.setMapOutputValueClass(Text.class);
	  	
	  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(Text.class);
	  	
	  	
	  	/******************* END****************************/
	  	System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
