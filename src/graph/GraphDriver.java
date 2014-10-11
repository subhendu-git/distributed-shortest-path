package graph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		boolean contd = true;
		int counter = 0;
		String inputPath = "/input";
		String outputPath = "/output";
		String output = outputPath + counter;
		while(contd){
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(GraphDriver.class);
			job.setMapperClass(GraphMapper.class);
			job.setReducerClass(GraphReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			int status = job.waitForCompletion(true)?0:1;
			if(status!=0){
				System.exit(status);
			}
			contd = false;
			inputPath = output + "/part-r-00000";
			FileSystem hdfs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputPath))));
			Map<Integer,Integer> pathMap = new HashMap<Integer,Integer>();
			String line = null;
			while((line = br.readLine())!=null){
				String[] tokens = line.split("\t| ");
				pathMap.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
			}
			br.close();
			Iterator<Integer> cost = pathMap.keySet().iterator();
			while(cost.hasNext()){
				if(pathMap.get((int)cost.next())>=Integer.MAX_VALUE){
					contd = true;
				}
			}
			counter++;
			inputPath = output;
			output = outputPath + counter;
		}
	}
}
