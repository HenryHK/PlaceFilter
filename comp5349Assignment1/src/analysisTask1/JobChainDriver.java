package analysisTask1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

//This is a program to chain different jobs of mappers and reducers work together.
//@author Zhao Liu

public class JobChainDriver 
{
	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) 
		{
			System.err.println("Usage: JobChainDriver <inPlace> <firstPhotoFile> <lastPhotoFile> <out>");
			System.exit(2);
		}
		
		// Pass a parameter to mapper class
//		if (otherArgs.length == 4){
//			conf.set("mapper.placeFilter.country", otherArgs[3]);
//		}
		
		//A temporary output path for the output of PlaceFilterMapper
		Path tmpFilterOut = new Path("tmpFilterOut");
		
		//A temporary output path for the output of TagsReducer
		Path tmpReducerOut = new Path("tmpReducerOut");
		
		//PlaceFilterMapper
		Job placeFilterJob = new Job(conf, "Place Filter");
		placeFilterJob.setJarByClass(PlaceFilterDriver.class);
		placeFilterJob.setNumReduceTasks(0);
		placeFilterJob.setMapperClass(PlaceFilterMapper.class);
		placeFilterJob.setOutputKeyClass(Text.class);
		placeFilterJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
		placeFilterJob.waitForCompletion(true);

		//ReplicateJoinMapper
		Job joinJob = new Job(conf, "Replication Join");
		DistributedCache.addCacheFile(new Path("tmpFilterOut/part-m-00000").toUri(),joinJob.getConfiguration());
		joinJob.setJarByClass(ReplicateJoinDriver.class);
		joinJob.setNumReduceTasks(10);
		joinJob.setMapperClass(ReplicateJoinMapper.class);
		joinJob.setReducerClass(TagsReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		
		//Record first and last file number
//		int first = Integer.parseInt(otherArgs[1]);
//		int last = Integer.parseInt(otherArgs[2]);
		
		//Combine several n0X.txt file into input 
//		for (int i = first; i <= last; i++)
//		{
//			String inputPath = "/share/data/large/n0" + i + ".txt";
//			TextInputFormat.addInputPath(joinJob, new Path(inputPath));
//		}
		
//		TextInputFormat.setInputPaths(joinJob, new Path(otherArgs[1]));
		TextInputFormat.addInputPath(joinJob, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(joinJob, tmpReducerOut);
		joinJob.waitForCompletion(true);
		
		//KeyChangerMapper
		Job keyChangerJob = new Job(conf, "Key Changer");
		keyChangerJob.setJarByClass(KeyChangerDriver.class);
		keyChangerJob.setNumReduceTasks(1);
		keyChangerJob.setMapperClass(KeyChangerMapper.class);
		keyChangerJob.setSortComparatorClass(InputKeySortComparator.class);
		keyChangerJob.setReducerClass(PhotosReducer.class);
		keyChangerJob.setOutputKeyClass(Text.class);
		keyChangerJob.setOutputValueClass(Text.class);
		
//		for(int i = 0; i < 10; i++)
//		{
//			String reducerPath = "tmpReducerOut/part-r-0000" + i;
//			TextInputFormat.addInputPath(keyChangerJob, new Path(reducerPath));
//		}
		
		
		
//		TextInputFormat.addInputPath(keyChangerJob, new Path("tmpReducerOut/part-r-00000"));
		
		TextInputFormat.setInputPaths(keyChangerJob, new Path("/user/zliu3708/tmpReducerOut"));
		
		TextOutputFormat.setOutputPath(keyChangerJob, new Path(otherArgs[2]));
		keyChangerJob.waitForCompletion(true);
		
		// remove the temporary path
		FileSystem.get(conf).delete(tmpFilterOut, true);
		FileSystem.get(conf).delete(tmpReducerOut, true);
	}
}
