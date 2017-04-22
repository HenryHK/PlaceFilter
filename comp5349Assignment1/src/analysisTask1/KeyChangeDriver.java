package analysisTask1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import analysisTask1.KeyChangeReducer;

public class  KeyChangeDriver{

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: PlaceFilter <in> <out>");
			System.exit(2);
		}

		Path tmpFilterReducerOut = new Path("temp");
		
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
		keyChangerJob.setJarByClass(KeyChangeDriver.class);
		keyChangerJob.setNumReduceTasks(1);
		keyChangerJob.setMapperClass(KeyChangeMapper.class);
		keyChangerJob.setSortComparatorClass(KeyInputComparator.class);
		keyChangerJob.setReducerClass(KeyChangeReducer.class);
		keyChangerJob.setOutputKeyClass(Text.class);
		keyChangerJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(keyChangerJob, new Path("/user/lhan9852/temp"));
		TextOutputFormat.setOutputPath(keyChangerJob, new Path(otherArgs[1]));
		keyChangerJob.waitForCompletion(true);

		FileSystem.get(conf).delete(tmpFilterReducerOut, true);

	}
}
