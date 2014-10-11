package graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\t| ");
		int dist = Integer.parseInt(tokens[1]) + 1;
		
		String[] vertices = tokens[2].split(":");
		for(int i=0;i<vertices.length;i++){
			context.write(new LongWritable(Integer.parseInt(vertices[i])), new Text("Val "+dist));
		}
		context.write(new LongWritable(Integer.parseInt(tokens[0])), new Text("Val "+tokens[1]));
		context.write(new LongWritable(Integer.parseInt(tokens[0])), new Text("Vertices "+tokens[2]));
	}
}
