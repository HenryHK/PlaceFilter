package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;

// input PlaceName (tag: freq)+
// output PlaceName 0 (tag: freq)+

public class TopTagJoinMapper extends Mapper<Object, Text, TextIntPair, Text>{
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if (data.length<2){
                return;
            }
            context.write(new TextIntPair(data[0],0), new Text(data[1]));
    }
}