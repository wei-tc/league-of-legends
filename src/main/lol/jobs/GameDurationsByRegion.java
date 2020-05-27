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

import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.POOR_NONENGLISH;
import static lol.util.RiftKeys.QUEUE_TYPE;
import static lol.util.RiftKeys.RANKED;
import static lol.util.RiftKeys.REGION;
import static lol.util.RiftKeys.RICH_ENGLISH;
import static lol.util.RiftKeys.UNRANKED;
import static lol.util.TimeProcessing.convertToMinutes;

public class GameDurationsByRegion extends Configured implements Tool
{
    public final static String USAGE = "usage: out";
    public final static VIntWritable ONE = new VIntWritable( 1 );
    private static final String INPUT = "/user/blackburn/riftwalk.gg/riftwalk.jsons";
    private static final String[] REGIONS = { "ranked_rich_english", "ranked_poor_nonenglish",
                                              "unranked_rich_english", "unranked_poor_nonenglish" };

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
            ToolRunner.run( new GameDurationsByRegion(), next );
        }
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "gameDurationsByRegion" );
        job.setJarByClass( GameDurationsByRegion.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        switch ( args[2] )
        {
            case "ranked_rich_english":
                job.setMapperClass( RankedRichMapper.class );
                break;
            case "ranked_poor_nonenglish":
                job.setMapperClass( RankedPoorMapper.class );
                break;
            case "unranked_rich_english":
                job.setMapperClass( UnrankedRichMapper.class );
                break;
            case "unranked_poor_nonenglish":
                job.setMapperClass( UnrankedPoorMapper.class );
                break;
            default:
                throw new IllegalArgumentException( USAGE );
        }
        job.setReducerClass( CountReducer.class );

        job.setMapOutputKeyClass( VIntWritable.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( VIntWritable.class );
        job.setOutputValueClass( VIntWritable.class );

        job.submit();
        return 0;
    }

    public static class RankedRichMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        private static final VIntWritable DURATION = new VIntWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DURATION.set( convertToMinutes( match.get( LENGTH ).toString() ) );
                context.write( DURATION, ONE );
            }
        }
    }

    public static class RankedPoorMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        private static final VIntWritable DURATION = new VIntWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DURATION.set( convertToMinutes( match.get( LENGTH ).toString() ) );
                context.write( DURATION, ONE );
            }
        }
    }

    public static class UnrankedRichMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        private static final VIntWritable DURATION = new VIntWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DURATION.set( convertToMinutes( match.get( LENGTH ).toString() ) );
                context.write( DURATION, ONE );
            }
        }
    }

    public static class UnrankedPoorMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        private static final VIntWritable DURATION = new VIntWritable();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DURATION.set( convertToMinutes( match.get( LENGTH ).toString() ) );
                context.write( DURATION, ONE );
            }
        }
    }

    public static class CountReducer extends Reducer<VIntWritable, VIntWritable, VIntWritable, VIntWritable>
    {
        private static final VIntWritable TOTAL = new VIntWritable();

        @Override
        protected void reduce( VIntWritable duration, Iterable<VIntWritable> count, Context context )
                throws IOException, InterruptedException
        {
            int total = 0;

            for ( VIntWritable c : count )
            {
                total++;
            }

            TOTAL.set( total );
            context.write( duration, TOTAL );
        }
    }
}
