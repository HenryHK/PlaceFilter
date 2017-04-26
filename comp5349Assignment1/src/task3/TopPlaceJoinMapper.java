package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;

// input PlaceName numOfPhotos
// output PlaceName 1 numOfPhotos

public class TopPlaceJoinMapper extends Mapper<Object, Text, TextIntPair, Text>{

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
            String[] data = value.toString().split("\t");
            if (data.length<2){
                return;
            }
            context.write(new TextIntPair(data[0], 1), new Text(data[1]));
        }

}