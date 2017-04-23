package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//input frequency \t PlaceName \t tags (sorted)
//output PlaceName \t frequency \t tags (top50)

public class TagReducer extends Reducer<Text, Text, Text, Text>{

    //HashMap<Text, Text> map = new HashMap<Text, Text>();
    private int counter = 0;

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            if(counter++ < 50){
                for(Text text: values){
                    String[] v = text.toString().split("\t");
                    context.write(new Text(v[0]), new Text(key.toString() + "\t" + v[1]));
                }
            }
    }

}