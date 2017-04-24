package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import task2.KeyChangeReducer;

public class  KeyChangeDriverWithTags{

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: PlaceFilter <in> <out>");
			System.exit(2);
		}

		Path tmpFilterReducerOut = new Path("temp1");
		
		Job job = new Job(conf, "Place Filter");
		job.setJarByClass(PlaceFilterDriver.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(PlaceFilterMapper.class);
		job.setReducerClass(PlaceFilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, tmpFilterReducerOut);
		job.waitForCompletion(true);

		Job keyChangerJob = new Job(conf, "Key Changer");
		keyChangerJob.setJarByClass(KeyChangeDriverWithTags.class);
		keyChangerJob.setNumReduceTasks(1);
		keyChangerJob.setMapperClass(TagMapper.class);
		keyChangerJob.setSortComparatorClass(KeyInputComparator.class);
		keyChangerJob.setReducerClass(TagReducer.class);
		keyChangerJob.setOutputKeyClass(Text.class);
		keyChangerJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(keyChangerJob, new Path("temp1"));
		TextOutputFormat.setOutputPath(keyChangerJob, new Path(otherArgs[1]));
		keyChangerJob.waitForCompletion(true);

		FileSystem.get(conf).delete(tmpFilterReducerOut, true);

	}
}
