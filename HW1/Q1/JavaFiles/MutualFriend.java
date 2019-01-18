package Q1MutualFriend;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.log4j.Logger;

public class MutualFriend {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntArrayWritable>{
		private Logger logger = Logger.getLogger(Map.class);
		//private Text key = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			String[] user = value.toString().split("\t");
			
			if(user.length >= 2) 
			{	
				IntWritable user1 = new IntWritable(Integer.parseInt(user[0]));	
				//IntWritable[] friends = (IntWritable[]) Arrays.asList(user[1].split(",")).stream().map(s -> new IntWritable(Integer.parseInt(s.trim()))).collect(Collectors.toList()).toArray();
				List<IntWritable> friends = Arrays.asList(user[1].split(",")).stream().map(s -> new IntWritable(Integer.parseInt(s.trim()))).collect(Collectors.toList());
				List<IntWritable> friendresult = Arrays.asList(user[1].split(",")).stream().map(s -> new IntWritable(Integer.parseInt(s.trim()))).collect(Collectors.toList());
				for(int i = 0; i < friends.size(); i++) 
				{
					Text intermediateKey = new Text();
					IntWritable friend = friends.get(i);
					if(user1.compareTo(friend) == -1) {
						intermediateKey.set(user1 + "_" + friend);
					}
					else if(user1.compareTo(friend) == 1) {
						intermediateKey.set(friend + "_" + user1);
					}
					else {
						continue;
					}
					friendresult.remove(friend);
					IntWritable[] a1 = new IntWritable[1];
					
					IntWritable[] a = friends.toArray(a1);
					logger.info(" a " + a);
					//IntArrayWritable result = new IntArrayWritable(a);
					IntArrayWritable result = new IntArrayWritable(IntWritable.class, a);

					//logger.info(" intermediate key "+ intermediateKey );
					//logger.info(" result " + result);
					context.write(intermediateKey, result);
					friendresult.add(friend);
				}
			}
			
		}

	}

	public static class IntArrayWritable extends ArrayWritable {

		public IntArrayWritable() {
			super(IntWritable.class);
		}
	    
		
	    public IntArrayWritable(IntWritable[] values) {
	        super(IntWritable.class, values);
	    }
	    
	    public IntArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
	        super(valueClass, values);
	    }
	    
	    public IntArrayWritable(Class<? extends Writable> valueClass) {
	        super(valueClass);
	    }
	    
	    @Override
	    public IntWritable[] get() {
	        return (IntWritable[]) super.get();
	    }
	    
	    @Override
	    public void write(DataOutput arg0) throws IOException {
	      System.out.println("write method called");
	      super.write(arg0);
	    }

	    @Override
	    public String toString() {
	        return Arrays.toString(get());
	    }
	}
	
	public static class Reduce extends Reducer<Text,IntArrayWritable,Text,Text> { 
		
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<IntArrayWritable> values,Context context) 
				throws IOException, InterruptedException 
		{
			String[] user = key.toString().split("_");
			int user1 = Integer.parseInt(user[0]);	
			int user2 = Integer.parseInt(user[1]);
			
			Text finalKey1 = new Text(user1 + "\t" + user2);
			//Text finalKey2 = new Text(user2 + "\t" + user1);
			List<IntWritable> mutualFriendList = new ArrayList<IntWritable>();
			
			ArrayList<List<IntWritable>> list = new ArrayList<List<IntWritable>>();
			for(IntArrayWritable friendSet : values) 
			{
				list.add(Arrays.asList( (IntWritable[]) friendSet.toArray()));
			}
			
			if(list.size() == 2) {
				Set<IntWritable> friends = new HashSet<IntWritable>(list.get(0));
				for(IntWritable mutualfriend : list.get(1)) {
					if(friends.contains(mutualfriend)) {
						mutualFriendList.add(mutualfriend);
					}
				}
			}
			
			if(!mutualFriendList.isEmpty()) {
				String value = StringUtils.join(',', mutualFriendList);
				Text finalValue = new Text(value);
				result.set(finalValue);
				context.write(finalKey1,result);// create a pair <keyword, number of occurences>
				//context.write(finalKey2,result);// create a pair <keyword, number of occurences>
			}
			
		}
	}


  	// Driver program
  	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
		  	System.err.println("Usage: MututalFriend <in> <out>");
		  	System.exit(2);
	  	}
	  	// create a job with name "MututalFriend"
	  	@SuppressWarnings("deprecation")
	  	Job job = new Job(conf, "MututalFriend");
	  	job.setJarByClass(MutualFriend.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	  	// set output key type
	  	
	  	job.setMapOutputKeyClass(Text.class);
	  	job.setMapOutputValueClass(IntArrayWritable.class);
	  	
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
