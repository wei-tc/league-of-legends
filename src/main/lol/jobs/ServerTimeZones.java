package lol.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import lol.util.TimeProcessing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static lol.util.RiftKeys.BANNED_CHAMPIONS;
import static lol.util.RiftKeys.END_TIME;
import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.REGION;

public class ServerTimeZones
{
    public final static String USAGE = "usage: out";
    private static final String INPUT = "/user/blackburn/riftwalk.gg/riftwalk.jsons";

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        Path in = new Path( INPUT );
        Path out = new Path( args[0] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "serverTimeZones" );
        job.setJarByClass( BannedChampionsByDayByRegion.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setMapperClass( TimeZoneMapper.class );
        job.setReducerClass( TimeZoneReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( Text.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        job.submit();
    }

    private static final Text ONE = new Text( "1" );

    public static class TimeZoneMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text DATE = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            context.write( new Text(match.get( END_TIME ).toString()), ONE );
        }
    }

    public static class TimeZoneReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce( Text time, Iterable<Text> champions, Context context )
                throws IOException, InterruptedException
        {
            context.write( time, ONE );
        }
    }
}
