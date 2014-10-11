package graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int min = Integer.MAX_VALUE;
		String vertices = "NotProcessed";
		for(Text value : values){
			String[] tokens = value.toString().split(" ");
			if(tokens[0].equals("Vertices")){
				vertices = tokens[1];
			}
			else if(tokens[0].equals("Val")){
				min = Math.min(Integer.parseInt(tokens[1]), min);
			}
		}
		context.write(key, new Text(min+" "+vertices));
	}
}
