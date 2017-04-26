package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import task3.KeyChangeReducer;
import task3.PlaceNumTagMapper;
import task3.PlaceNumTagReducer;

public class  PlaceNumTagDriver{

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: PlaceNumTag <in> <out>");
			System.exit(2);
		}

		Job keyChangerJob = new Job(conf, "PlaceNumTag");
		keyChangerJob.setJarByClass(PlaceNumTagDriver.class);
		keyChangerJob.setNumReduceTasks(1);
		keyChangerJob.setMapperClass(PlaceNumTagMapper.class);
		keyChangerJob.setSortComparatorClass(KeyInputComparator.class);
		keyChangerJob.setReducerClass(PlaceNumTagReducer.class);
		keyChangerJob.setOutputKeyClass(Text.class);
		keyChangerJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(keyChangerJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(keyChangerJob, new Path(otherArgs[1]));
		keyChangerJob.waitForCompletion(true);

		//FileSystem.get(conf).delete(tmpFilterReducerOut, true);

	}
}
