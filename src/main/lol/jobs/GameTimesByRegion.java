package lol.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static lol.util.RiftKeys.END_TIME;
import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.POOR_NONENGLISH_SA;
import static lol.util.RiftKeys.REGION;
import static lol.util.RiftKeys.RICH_ENGLISH_NA;
import static lol.util.TimeProcessing.getStartTime;

public class GameTimesByRegion extends Configured implements Tool
{
    public final static String USAGE = "usage: out";
    public final static VIntWritable ONE = new VIntWritable( 1 );
    private static final String INPUT = "/user/blackburn/riftwalk.gg/riftwalk.jsons";
    private static final String[] REGIONS = { "rich_english", "poor_nonenglish" };

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        for ( String r : REGIONS )
        {
            String[] next = new String[3];
            next[0] = INPUT;
            next[1] = args[0] + r;
            next[2] = r;
            ToolRunner.run( new GameTimesByRegion(), next );
        }
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "gameTimesByRegion" );
        job.setJarByClass( GameTimesByRegion.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );
        switch ( args[2] )
        {
            case "rich_english":
                job.setMapperClass( RichMapper.class );
                break;
            case "poor_nonenglish":
                job.setMapperClass( PoorMapper.class );
                break;
            default:
                throw new IllegalArgumentException( USAGE );
        }
        job.setReducerClass( CountReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VIntWritable.class );

        job.submit();
        return 0;
    }

    public static class RichMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private static final Text START_TIME = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH_NA.contains( match.get( REGION ).toString() ) )
            {
                START_TIME.set( getStartTime( match.get( END_TIME ).toString(),
                                              match.get( LENGTH ).toString() ) );
                context.write( START_TIME, ONE );
            }
        }
    }

    public static class PoorMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private static final Text START_TIME = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH_SA.contains( match.get( REGION ).toString() ) )
            {
                START_TIME.set( getStartTime( match.get( END_TIME ).toString(),
                                              match.get( LENGTH ).toString() ) );
                context.write( START_TIME, ONE );
            }
        }
    }

    public static class CountReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {
        private static final VIntWritable TOTAL = new VIntWritable();

        @Override
        protected void reduce( Text startTime, Iterable<VIntWritable> count, Context context )
                throws IOException, InterruptedException
        {
            int total = 0;

            for ( VIntWritable c : count )
            {
                total++;
            }

            TOTAL.set( total );
            context.write( startTime, TOTAL );
        }
    }
}
