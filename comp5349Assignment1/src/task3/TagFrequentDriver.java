package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import task3.TagFrequentMapper;
import task3.TagFrequentReducer;
import task3.TextTextPair;
import task3.TopTagMapper;
import task3.TopTagSortComparator;

public class  TagFrequentDriver{

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: TagFrequentDriver <in> <out>");
			System.exit(2);
		}
		
		//Configuration config = new Configuration(); 
		conf.set("mapreduce.map.java.opts", "-Xmx2048M"); //sorry for this but I do require a large memory
		//JobConf conf1 = new JobConf(config, this.getClass());
		Job job = new Job(conf, "TagFrequentDriver");
		//job.setNumMapTasks(5);
		job.setJarByClass(TagFrequentDriver.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(TagFrequentMapper.class);
		job.setReducerClass(TagFrequentReducer.class);
		job.setOutputKeyClass(TextTextPair.class);
        //job.setSortComparatorClass(TopTagSortComparator.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

	}
}
