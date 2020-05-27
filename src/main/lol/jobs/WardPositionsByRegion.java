package lol.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static lol.util.RiftKeys.DATA;
import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.POOR_NONENGLISH;
import static lol.util.RiftKeys.QUEUE_TYPE;
import static lol.util.RiftKeys.RANKED;
import static lol.util.RiftKeys.REGION;
import static lol.util.RiftKeys.RICH_ENGLISH;
import static lol.util.RiftKeys.UNRANKED;
import static lol.util.RiftKeys.WARDFRAMES;
import static lol.util.RiftKeys.WARD_END_TIME;
import static lol.util.RiftKeys.WARD_START_TIME;
import static lol.util.RiftKeys.X;
import static lol.util.RiftKeys.Y;

public class WardPositionsByRegion extends Configured implements Tool
{
    public final static String USAGE = "usage: out";
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
            ToolRunner.run( new WardPositionsByRegion(), next );
        }
    }

    public static String threeDP( double time )
    {
        int factor = (int) Math.pow( 10, 3 );
        return Double.toString( 1.0 * Math.round( time * factor ) / factor );
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "wardPositionsByRegion" );
        job.setJarByClass( WardPositionsByRegion.class );

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

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( Text.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        job.submit();
        return 0;
    }

    public static class RankedRichMapper extends Mapper<Object, Text, Text, Text>
    {
        private static final Text START_END_TIME = new Text();
        private static final Text COORDINATES = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                Any wardframes = match.get( DATA ).get( WARDFRAMES );

                if ( wardframes.size() > 0 )
                {
                    double length = match.get( LENGTH ).toDouble();

                    for ( Any ward : wardframes )
                    {
                        START_END_TIME.set( threeDP( ward.get( WARD_START_TIME ).toDouble() / length )
                                            + " "
                                            + threeDP( ward.get( WARD_END_TIME ).toDouble() / length ) );

                        COORDINATES.set( ward.get( X ).toString() + " "
                                         + ward.get( Y ).toString() );

                        context.write( START_END_TIME, COORDINATES );
                    }
                }
            }
        }
    }

    public static class RankedPoorMapper extends Mapper<Object, Text, Text, Text>
    {
        private static final Text START_END_TIME = new Text();
        private static final Text COORDINATES = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                Any wardframes = match.get( DATA ).get( WARDFRAMES );

                if ( wardframes.size() > 0 )
                {
                    double length = match.get( LENGTH ).toDouble();

                    for ( Any ward : wardframes )
                    {
                        START_END_TIME.set( threeDP( ward.get( WARD_START_TIME ).toDouble() / length )
                                            + " "
                                            + threeDP( ward.get( WARD_END_TIME ).toDouble() / length ) );

                        COORDINATES.set( ward.get( X ).toString() + " "
                                         + ward.get( Y ).toString() );

                        context.write( START_END_TIME, COORDINATES );
                    }
                }
            }
        }
    }

    public static class UnrankedRichMapper extends Mapper<Object, Text, Text, Text>
    {
        private static final Text START_END_TIME = new Text();
        private static final Text COORDINATES = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                Any wardframes = match.get( DATA ).get( WARDFRAMES );

                if ( wardframes.size() > 0 )
                {
                    double length = match.get( LENGTH ).toDouble();

                    for ( Any ward : wardframes )
                    {
                        START_END_TIME.set( threeDP( ward.get( WARD_START_TIME ).toDouble() / length )
                                            + " "
                                            + threeDP( ward.get( WARD_END_TIME ).toDouble() / length ) );

                        COORDINATES.set( ward.get( X ).toString() + " "
                                         + ward.get( Y ).toString() );

                        context.write( START_END_TIME, COORDINATES );
                    }
                }
            }
        }
    }

    public static class UnrankedPoorMapper extends Mapper<Object, Text, Text, Text>
    {
        private static final Text START_END_TIME = new Text();
        private static final Text COORDINATES = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                Any wardframes = match.get( DATA ).get( WARDFRAMES );

                if ( wardframes.size() > 0 )
                {
                    double length = match.get( LENGTH ).toDouble();

                    for ( Any ward : wardframes )
                    {
                        START_END_TIME.set( threeDP( ward.get( WARD_START_TIME ).toDouble() / length )
                                            + " "
                                            + threeDP( ward.get( WARD_END_TIME ).toDouble() / length ) );

                        COORDINATES.set( ward.get( X ).toString() + " "
                                         + ward.get( Y ).toString() );

                        context.write( START_END_TIME, COORDINATES );
                    }
                }
            }
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text>
    {
        private static final Text OUTPUT = new Text();

        @Override
        protected void reduce( Text startEndTime, Iterable<Text> coordinates, Context context )
                throws IOException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();

            for ( Text coordinate : coordinates )
            {
                sb.append( coordinate.toString() ).append( " " );
            }
            sb.setLength( sb.length() - 1 ); // remove extra space

            OUTPUT.set( sb.toString() );
            context.write( startEndTime, OUTPUT );
        }
    }
}
