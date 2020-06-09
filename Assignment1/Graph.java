import java.io.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.*;

public class Graph 
{
    public static class MyMapper extends Mapper<Object,Text,LongWritable,LongWritable> 
    {
        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long x = s.nextLong();
            long y = s.nextLong();
            context.write(new LongWritable(x), new LongWritable(y));
            s.close();
        }
    }

    public static class MyReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> 
    {
        @Override
        public void reduce (LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {
        	long count=0;
        	for(LongWritable n: values)
        	{
        		count++;
        	}
        	context.write(key, new LongWritable(count));
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>
    {
    	public void map(LongWritable values, LongWritable key, Context context) throws IOException, InterruptedException
    	{
			context.write(key,new LongWritable(1));
    	}
    }

    public static class MyReducer2 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
    {
    	public void reduce (LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {

        	System.out.println("INSIDE REDUCER"+key);
    		long sum=0;
        	for(LongWritable n: values)
        	{
        		System.out.println("INSIDE REDUCER INSIDE FOR LOOP"+ n);
        		sum = sum + n.get();
        	}
        	context.write(key, new LongWritable(sum));
    	}
    }

    public static void main (String[] args) throws Exception 
    {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path("/tmp/rxs"));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("/tmp/rxs"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);
    }
}