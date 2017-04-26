package task3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import task3.TextIntPair;

public class TopTagJoinReducer extends Reducer<TextIntPair, Text, Text, Text>{

    public void reduce(TextIntPair key, Iterable<Text> values, 
            Context context) throws IOException, InterruptedException{

                Iterator<Text> valueItr = values.iterator();
                if(key.getOrder().get()==0){ //come from tag
                    String first = valueItr.next().toString();
                    while(valueItr.hasNext()){
                        Text value = new Text(valueItr.next().toString() + "\t" + first);
                        context.write(key.getKey(), value);
                    }
                }else{
                    while(valueItr.hasNext()){
                        context.write(key.getKey(), new Text(valueItr.next().toString() + "\t" + "NULL"));
                    }
                }

    }

}