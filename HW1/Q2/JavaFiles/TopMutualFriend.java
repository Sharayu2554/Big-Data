package Q2TopMutualFriend;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.log4j.Logger;

public class TopMutualFriend {
	
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
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntArrayWritable>{
		private Logger logger = Logger.getLogger(Map.class);

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
				Text finalValue = new Text(value + "\t" + mutualFriendList.size());
				result.set(finalValue);
				context.write(finalKey1,result);// create a pair <keyword, number of occurences>
				//context.write(finalKey2,result);// create a pair <keyword, number of occurences>
			}
			
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
		//private Logger logger = Logger.getLogger(Map2.class);
		Queue<MutualFriend> queue = new PriorityQueue<MutualFriend>();
		Integer max = 0;
		Integer count = 0;
		public static final Integer COUNTER = 10;
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			String[] values = value.toString().split("\t");
			if(values.length == 4 ) {
				MutualFriend m = 
					new MutualFriend(
							Integer.parseInt(values[3]), Integer.parseInt(values[0]), 
							Integer.parseInt(values[1]), values[2]
					);
				
				if(count < COUNTER) {
					//logger.info("\nAdded MutualFriend "  + m.toString());
					queue.add(m);
					count++;
					if(Integer.parseInt(values[3]) > max) {
						max = Integer.parseInt(values[3]);
					}
				}
				else {

					if(Integer.parseInt(values[3]) > max) {
						MutualFriend r =  queue.remove();
						//logger.info("Removed r " + r.toString());
						//logger.info("\nAdded MutualFriend "  + m.toString());
						queue.add(m);
						
						max = Integer.parseInt(values[3]);
					}
					else if(Integer.parseInt(values[3]) > queue.peek().getCount()){
						MutualFriend r =  queue.remove();	
						//logger.info("Removed r " + r.toString());
						//logger.info("\nAdded MutualFriend "  + m.toString());
						queue.add(m);
					}
				}
				
			}
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			
			MutualFriend friend = new MutualFriend();
			int queueSize = queue.size();
			for(int i = 0; i < queueSize; i++) 
			{
				friend = queue.remove();
				//logger.info("Sending Count  "+ friend.getCount().toString());
				//logger.info("Sending friend "+ new String(friend.getUser1() + "\t" + friend.getUser2() + "\t" + friend.getFriends()));
				context.write(
					new IntWritable(friend.getCount()), 
					new Text(new String(friend.getUser1() + "\t" + friend.getUser2() + "\t" + friend.getFriends()))
				);
			}

		}
	}
	
	public static class DescendingIntComparator extends WritableComparator {
		//private Logger logger = Logger.getLogger(DescendingIntComparator.class);
		
		public DescendingIntComparator() {
			super(IntWritable.class, true);
		}
		
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
        	//logger.info("comparator w1 " + w1 + " w2 " + w2);
        	if(w1 != null && w2 != null) {
	            IntWritable key1 = (IntWritable) w1;
	            IntWritable key2 = (IntWritable) w2;          
	            return -1 * key1.compareTo(key2);
        	}
        	return 0;
        }
    }

	public static class Reduce2 extends Reducer<IntWritable,Text,NullWritable,Text> { 
		//private Logger logger = Logger.getLogger(Reduce2.class);
		
		public void reduce(IntWritable key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException 
		{
			for(Text value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}


  	// Driver program
  	public static void main(String[] args) throws Exception {
  		
  		/*************** JOB 1 *****************/
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	  	if (otherArgs.length != 3) {
		  	System.err.println("Usage: TopMututalFriend <in> <temp_out> <out>");
		  	System.exit(2);
	  	}
	  	
	  	@SuppressWarnings("deprecation")
	  	Job job = new Job(conf, "TopMututalFriend");
	  	job.setJarByClass(TopMutualFriend.class);
	  	
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	
	  	job.setMapOutputKeyClass(Text.class);
	  	job.setMapOutputValueClass(IntArrayWritable.class);
	  	
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	
	  	job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	  	
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	
	  	job.waitForCompletion(true);
	  	
	  	/************** JOB 2 ********************/
	  	
	  	Configuration conf2 = new Configuration();
	  	
	  	@SuppressWarnings("deprecation")
	  	Job job2 = new Job(conf2, "TopMututalFriend2");
	  	job2.setJarByClass(TopMutualFriend.class);
	  	 	
	  	FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
	  	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2])); 
	  	
	  	//job.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

	  	job2.setMapperClass(Map2.class);
	  	job2.setReducerClass(Reduce2.class);
	  	
	  	job2.setMapOutputKeyClass(IntWritable.class);
	  	job2.setMapOutputValueClass(Text.class);
	  	
	  	job2.setSortComparatorClass(DescendingIntComparator.class); 
	  	
	  	job2.setOutputKeyClass(NullWritable.class);
	  	job2.setOutputValueClass(Text.class);

	  	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  	}
  }
