package task3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import task3.TextIntPair;

// input place-id mapper#0 place-name place-type-id
//       place-id mapper#1 tags

// output place-id tags place-name place-type-id 

public class JoinReducer extends Reducer<TextIntPair, Text, Text, Text>{

    public void reduce(TextIntPair key, Iterable<Text> values, 
            Context context) throws IOException, InterruptedException{
                Iterator<Text> valueItr = values.iterator();
                if(key.getOrder().get() == 0){ //the key is from place.txt
                    String first = valueItr.next().toString();
                    while(valueItr.hasNext()){
                        Text value = new Text(valueItr.next().toString() + "\t" + first);
                        context.write(key.getKey(), value);
                    }
                }else{ //the key is from photo
                    while(valueItr.hasNext()){
                        context.write(key.getKey(), new Text(valueItr.next().toString() + "\t" + "NULL"));
                    }
                }
    }

}