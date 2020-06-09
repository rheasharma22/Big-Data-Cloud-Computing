import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

class Elem implements Writable
{
	int tag; 
	int index;
	double value;

	Elem() {}

	Elem (int tag, int index, double value)
	{
		this.tag = tag;
		this.index = index;
		this.value = value;
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(tag);
		out.writeInt(index);
		out.writeDouble(value);
	}

	public void readFields(DataInput in) throws IOException
	{
		tag = in.readInt();
		index = in.readInt();
		value = in.readDouble();
	}
}

class Pair implements WritableComparable<Pair>
{
	int i;
	int j;

	Pair() {}

	Pair(int i, int j)
	{
		this.i=i;
		this.j=j;
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(i);
		out.writeInt(j);
	}

	public void readFields(DataInput in) throws IOException
	{
		i = in.readInt();
		j = in.readInt();
	}

	public int compareTo(Pair compare) 
	{
		if(this.i < compare.i) 
			return -1;
		else if(this.i > compare.i) 
			return 1;
		else if(this.j < compare.j)
			return -1;
		else if(this.j > compare.j)
			return 1;
		return 0;
	}

	public String toString() 
	{
		return i + " " + j + " ";
	}
}

public class Multiply 
{
	public static class Mapper1 extends Mapper<Object, Text, IntWritable, Elem>
	{
		@Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException 
        {
        	Scanner str = new Scanner(value.toString()).useDelimiter(",");
			int a = str.nextInt();
			int b = str.nextInt();
			Double v = str.nextDouble();

			IntWritable keyVal = new IntWritable(b);
			Elem e = new Elem(0,a,v);
            context.write(keyVal,e);
            str.close();
        }
	}

	public static class Mapper2 extends Mapper<Object, Text, IntWritable, Elem>
	{
		@Override
        public void map (Object key, Text value, Context context ) throws IOException, InterruptedException 
        {
        	Scanner str = new Scanner(value.toString()).useDelimiter(",");
            int a = str.nextInt();
			int b = str.nextInt();
			Double v = str.nextDouble();
			
			IntWritable keyVal = new IntWritable(a);
			Elem e = new Elem(1,b,v);
            context.write(keyVal,e);
            str.close();
        }
	}

	public static class Reducer1 extends Reducer<IntWritable, Elem, Pair, DoubleWritable>
	{
		Vector<Elem> A = new Vector<Elem>();
		Vector<Elem> B = new Vector<Elem>();
        @Override
        public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException 
        {
        	A.clear();
        	B.clear();
		
			for(Elem ele : values) 
			{			
				if (ele.tag == 0) 
				{
					Elem e = new Elem(ele.tag, ele.index, ele.value);
					A.add(e);
				} 
				else if(ele.tag == 1) 
				{
					Elem e = new Elem(ele.tag, ele.index, ele.value);
					B.add(e);
				}
			}
			
			int x, y;
			for(x=0; x<A.size(); x++) 
			{
				for(y=0; y<B.size(); y++) 
				{
					Pair p = new Pair(A.get(x).index, B.get(y).index);
					context.write(p, new DoubleWritable(A.get(x).value * B.get(y).value));
				}
			}
		}
	}

    public static class MapperMultiply extends Mapper<Object, DoubleWritable, Pair, DoubleWritable>
    {
    	public void map(Object key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
    	{
    		Scanner str = new Scanner(values.toString()).useDelimiter(" ");
            
            String a = str.next();
			String b = str.next();
			String v = str.next();
			
			Pair p = new Pair(Integer.parseInt(a),Integer.parseInt(b));
			DoubleWritable value = new DoubleWritable(Double.parseDouble(v));

			context.write(p, value);
			str.close();
    	}
    }

    public static class ReducerMultiply extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable>
    {
    	public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
    	{
			double add = 0.0;
			for(DoubleWritable value : values) 
			{
				add = add + value.get();
			}
			context.write(key, new DoubleWritable(add));
		}
	}

    public static void main ( String[] args ) throws Exception
    {
    	Job job = Job.getInstance();
        job.setJobName("Multiply Intermediate");
        job.setJarByClass(Multiply.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mapper2.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Multiply Output");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(MapperMultiply.class);
        job2.setReducerClass(ReducerMultiply.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}