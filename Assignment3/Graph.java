import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.lang.*;


class Vertex implements Writable 
{
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors

    Vertex() {}

    Vertex(short tag, long group, long VID, Vector<Long> adjacent)
    {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex(short tag, long group)
    {
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        this.adjacent = new Vector();
    }

    public void write(DataOutput out) throws IOException
    {
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);

        int size = adjacent.size();
        out.writeInt(adjacent.size());

        for(int i=0; i<size; i++)
            out.writeLong(adjacent.get(i));
    }

    public void readFields(DataInput in) throws IOException 
    {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        adjacent = new Vector<Long>();
        int size = in.readInt();
        for (int i=0; i<size; i++)
            adjacent.add(in.readLong());
    }

    @Override
    public String toString()
    {
        return "VID: " + VID + "Adjacent node: "+ adjacent + "Group No.: " + group + "Tag: " + tag;
    }
}

public class Graph 
{
    public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex>
    {
        @Override
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException
        {
            String str = line.toString();
            String s[] = str.split(",");
            long vid = Long.parseLong(s[0]);
            Vertex v = new Vertex();
            v.VID = vid;
            v.adjacent = new Vector<Long>();
            v.tag = (short) 0;
            v.group = vid;

            for(int i=0; i<s.length; i++)
            {
                v.adjacent.add(Long.parseLong(s[i]));
            }
            //System.out.println(v.adjacent);
            context.write(new LongWritable(vid), v);
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex>
    {
        @Override
        public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException
        {
            context.write(new LongWritable(vertex.VID), vertex);
            //System.out.println(vertex.adjacent.size());
            Iterator<Long> adjacent = vertex.adjacent.iterator();
            for(int n=0; n<vertex.adjacent.size(); n++)
            {
                //System.out.println("INSIDE FOR LOOP MAPPER 2");
                context.write(new LongWritable(adjacent.next()), new Vertex((short)1, vertex.group));
            }
        }
    }

    public static class Reducer1 extends Reducer<LongWritable, Vertex, LongWritable, Vertex>
    {
        @Override
        public void reduce(LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException
        {
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector<Long>();
            for(Vertex vert: values)
            {
                //System.out.println("REDUCER 1 FOR LOOP");
                if(vert.tag == 0)
                {
                    adj = (Vector)vert.adjacent.clone();
                }
                m = Math.min(m, vert.group);
            }
            //System.out.println("REDUCER 1!"+m);
            context.write(new LongWritable(m), new Vertex((short)0, m, vid.get(), adj));

            //System.out.println("---------- REDUCER 1 ----------");
        }
    }

    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable>
    {
        @Override
        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException
        {
            context.write(key, new IntWritable(1));
        }
    }

    public static class FinalReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>
    {
        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int m = 0;
            for(IntWritable v: values)
            {
                m = m + v.get();
            }
            context.write(key, new IntWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception 
    {
        Job job = Job.getInstance();
        job.setJobName("MyJob1");
        job.setJarByClass(Graph.class);
        
        job.setMapperClass(Mapper1.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]+"/f0"));
        
        job.waitForCompletion(true);

        /* ... First Map-Reduce job to read the graph */
        
        for (short i=0; i<5; i++) 
        {
            /* ... Second Map-Reduce job to propagate the group number */
            job = Job.getInstance();
            job.setJobName("MyJob2");
            job.setJarByClass(Graph.class);
        
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);

            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer1.class);
        
            SequenceFileInputFormat.setInputPaths(job, new Path(args[1]+"/f"+i));
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]+"/f"+(i+1)));
            job.waitForCompletion(true);
        }

        job = Job.getInstance();
        job.setJobName("MyJob3");
        job.setJarByClass(Graph.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);

        SequenceFileInputFormat.setInputPaths(job, new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        /* ... Final Map-Reduce job to calculate the connected component sizes */

        job.waitForCompletion(true);
    }
}