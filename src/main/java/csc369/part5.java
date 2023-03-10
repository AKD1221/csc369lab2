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

public class part5 {

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
            String month = new String(sa[3].substring(1).split("/")[1]);
            String year = new String(sa[3].substring(1).split("/")[2].split(":")[0]);


            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(month);//put your month name in english here
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            Integer monthNumber= cal.get(Calendar.MONTH) + 1;

            String formatted = String.format("%02d", monthNumber);


            hostname.set(year + formatted);
	        context.write(hostname, one);
    
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
