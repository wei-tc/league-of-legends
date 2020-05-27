package lol.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import lol.util.RiftKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static lol.util.RiftKeys.QUEUE_TYPE;
import static lol.util.RiftKeys.RANKED;
import static lol.util.RiftKeys.UNRANKED;

public class MaxGameLengthByRegionByGameType extends Configured implements Tool
{
    private static final String INPUT = "/user/blackburn/riftwalk.gg/riftwalk.jsons";
    private static final String USAGE = "usage: out";
    private static final String[] GAME_TYPES = { "ranked", "unranked" };

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        for ( String t : GAME_TYPES )
        {
            String[] next = new String[3];
            next[0] = INPUT;
            next[1] = args[0] + t;
            next[2] = t;
            ToolRunner.run( new MaxGameLengthByRegionByGameType(), next );
        }
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "MaxGameLengthByRegionByGameType" );
        job.setJarByClass( MaxGameLengthByRegionByGameType.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        switch ( args[2] )
        {
            case "ranked":
                job.setMapperClass( RankedLengthMapper.class );
                break;
            case "unranked":
                job.setMapperClass( UnrankedLengthMapper.class );
                break;
            default:
                throw new IllegalArgumentException( USAGE );
        }
        job.setReducerClass( LengthReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( DoubleWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( DoubleWritable.class );

        job.submit();
        return 0;
    }

    public static class RankedLengthMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        private final static Text REGION = new Text();
        private final static DoubleWritable LENGTH = new DoubleWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                REGION.set( match.get( RiftKeys.REGION ).toString() );
                LENGTH.set( match.get( RiftKeys.LENGTH ).toDouble() );
                context.write( REGION, LENGTH );
            }
        }
    }

    public static class UnrankedLengthMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        private final static Text REGION = new Text();
        private final static DoubleWritable LENGTH = new DoubleWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                REGION.set( match.get( RiftKeys.REGION ).toString() );
                LENGTH.set( match.get( RiftKeys.LENGTH ).toDouble() );
                context.write( REGION, LENGTH );
            }
        }
    }

    public static class LengthReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        private final static DoubleWritable MAX_LENGTH = new DoubleWritable();

        @Override
        protected void reduce( Text region, Iterable<DoubleWritable> lengths, Context context )
                throws IOException, InterruptedException
        {
            double maxLength = 0;

            for ( DoubleWritable l : lengths )
            {
                double length = l.get();
                if ( length > maxLength )
                {
                    maxLength = length;
                }
            }

            MAX_LENGTH.set( maxLength );
            context.write( region, MAX_LENGTH );
        }
    }
}
