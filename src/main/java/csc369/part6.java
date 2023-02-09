package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.text.SimpleDateFormat;  

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.protobuf.TextFormat.ParseException;

public class part6 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {

        try{
            
            String[] sa = value.toString().split(" ");
            Text hostname = new Text();
            String datetime = new String(sa[3].substring(1));
            String mydate = new String(datetime.split(":")[0]);
            IntWritable myval = new IntWritable(Integer.parseInt(sa[9]));

        
            SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
            Date date = format.parse(mydate);

            SimpleDateFormat dayOfWeekFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);
            String dayOfWeek = dayOfWeekFormat.format(date);



            hostname.set(dayOfWeek);
	        context.write(hostname, myval);
    
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }


        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text hostname, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(hostname, result);
       }
    }

}
