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
import static lol.util.RiftKeys.CHAMPION_IDS;
import static lol.util.RiftKeys.END_TIME;
import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.MAP_ID;
import static lol.util.RiftKeys.QUEUE_TYPE;
import static lol.util.RiftKeys.SUMMONERS_RIFT;

public class BannedChampions
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
        Job job = Job.getInstance( conf, "bannedChampions" );
        job.setJarByClass( BannedChampionsByDayByRegion.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setMapperClass( BannedChampionsMapper.class );
        job.setReducerClass( ChampionsReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VIntWritable.class );

        job.submit();
    }

    public final static VIntWritable ONE = new VIntWritable( 1 );

    public static class BannedChampionsMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );
            if ( match.get( QUEUE_TYPE ).toString().equals( "9" ))
            {
                Any champions = match.get( CHAMPION_IDS );

                for ( Any champion : champions )
                {
                    context.write( new Text( champion.toString() ), ONE );
                }
            }
        }
    }


    public static class ChampionsReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {

        @Override
        protected void reduce( Text champion, Iterable<VIntWritable> count, Context context )
                throws IOException, InterruptedException
        {
            int total = 0;

            for ( VIntWritable c : count )
            {
                total++;
            }

            context.write( champion, new VIntWritable( total ) );
        }
    }
}