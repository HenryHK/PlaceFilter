package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3Driver{

    /*
    
    Task3.1
    hadoop jar PlaceFilter.jar task3.TagDriver Task1/* Task3.1

    Task3.2
    hadoop jar PlaceFilter.jar task3.TagFrequentDriver Task3.1/* Task3.2

    Task3.3
    hadoop jar PlaceFilter.jar task3.TopTagDriver Task3.2/* Task3.3

    Task3.4
    hadoop jar PlaceFilter.jar task3.TopTagPlaceJoinDriver Task2/* Task3.3/* Task3.4

    Task3.5
    hadoop jar PlaceFilter.jar task3.PlaceNumTagDriver Task3.4/* Task3.5

    */

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.java.opts", "-Xmx2048M");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 3) {
			System.err.println("Usage: PlaceFilter <task1out> <task2out> <out>");
			System.exit(2);
		}

        Path task3aTemp = new Path("tempTask3.1");
        Path task3bTemp = new Path("tempTask3.2");
        Path task3cTemp = new Path("tempTask3.3");
        Path task3dTemp = new Path("tempTask3.4");
        // Path task3eTemp = new Path("Task3.5");

        //task3.1
        Job job1 = new Job(conf, "Tag Driver");
		job1.setJarByClass(TagDriver.class);
		job1.setNumReduceTasks(1);
		job1.setMapperClass(TagMapper.class);
		job1.setReducerClass(TagReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
        job1.setSortComparatorClass(KeyInputComparator.class);
		TextInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job1, task3aTemp);
		job1.waitForCompletion(true);

        //task3.2
        Job job2 = new Job(conf, "TagFrequentDriver");
		job2.setJarByClass(TagFrequentDriver.class);
		job2.setNumReduceTasks(2);
		job2.setMapperClass(TagFrequentMapper.class);
		job2.setCombinerClass(TagFrequentCombiner.class);
		job2.setReducerClass(TagFrequentReducer.class);
		job2.setOutputKeyClass(TextTextPair.class);
		job2.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job2, task3aTemp);
		TextOutputFormat.setOutputPath(job2, task3bTemp);
		job2.waitForCompletion(true);

        //task3.3
        Job job3 = new Job(conf, "TopTagDriver");
		job3.setJarByClass(TopTagDriver.class);
		job3.setNumReduceTasks(2);
		job3.setMapperClass(TopTagMapper.class);
		job3.setReducerClass(TopTagReducer.class);
		job3.setOutputKeyClass(TextIntPair.class);
        job3.setSortComparatorClass(TopTagSortComparator.class);
		job3.setGroupingComparatorClass(TopTagGroupingComparator.class);
		job3.setPartitionerClass(TopTagPartitioner.class);
		job3.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job3, task3bTemp);
		TextOutputFormat.setOutputPath(job3, task3cTemp);
		job3.waitForCompletion(true);

        //task3.4
        Job job4 = new Job(conf, "join top tag and place");
		job4.setNumReduceTasks(4); 
		job4.setJarByClass(TopTagPlaceJoinDriver.class);
		
		Path tagTable = task3cTemp;
		Path placeTable = new Path(otherArgs[1]);
		Path outputTable = task3dTemp;
		MultipleInputs.addInputPath(job4, tagTable, 
				TextInputFormat.class,TopTagJoinMapper.class);
		MultipleInputs.addInputPath(job4, placeTable, TextInputFormat.class,TopPlaceJoinMapper.class);
		FileOutputFormat.setOutputPath(job4, outputTable);
		job4.setMapOutputKeyClass(TextIntPair.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setGroupingComparatorClass(TagPlaceJoinGroupingComparator.class);
		job4.setReducerClass(TopTagJoinReducer.class);
		job4.setPartitionerClass(TagPlaceJoinPartitioner.class);
		job4.waitForCompletion(true);

        //task3.5
        Job keyChangerJob = new Job(conf, "PlaceNumTag");
		keyChangerJob.setJarByClass(PlaceNumTagDriver.class);
		keyChangerJob.setNumReduceTasks(1);
		keyChangerJob.setMapperClass(PlaceNumTagMapper.class);
		keyChangerJob.setSortComparatorClass(KeyInputComparator.class);
		keyChangerJob.setReducerClass(PlaceNumTagReducer.class);
		keyChangerJob.setOutputKeyClass(Text.class);
		keyChangerJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(keyChangerJob, task3dTemp);
		TextOutputFormat.setOutputPath(keyChangerJob, new Path(otherArgs[2]));
		keyChangerJob.waitForCompletion(true);
    }
}