package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import task3.TextTextPair;
import task3.TopTagGroupingComparator;
import task3.TopTagMapper;
import task3.TopTagPartitioner;
import task3.TopTagSortComparator;

public class  TopTagDriver{

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: TopTagDriver <in> <out>");
			System.exit(2);
		}
		

		Job job = new Job(conf, "TopTagDriver");
		job.setJarByClass(TopTagDriver.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(TopTagMapper.class);
		job.setReducerClass(TopTagReducer.class);
		job.setOutputKeyClass(TextIntPair.class);
        job.setSortComparatorClass(TopTagSortComparator.class);
		job.setGroupingComparatorClass(TopTagGroupingComparator.class);
		job.setPartitionerClass(TopTagPartitioner.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

	}
}
