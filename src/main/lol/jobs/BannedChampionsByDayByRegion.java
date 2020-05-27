package lol.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import lol.util.TimeProcessing;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static lol.util.RiftKeys.BANNED_CHAMPIONS;
import static lol.util.RiftKeys.BANNED_CHAMPION_ID;
import static lol.util.RiftKeys.END_TIME;
import static lol.util.RiftKeys.LENGTH;
import static lol.util.RiftKeys.POOR_NONENGLISH;
import static lol.util.RiftKeys.QUEUE_TYPE;
import static lol.util.RiftKeys.RANKED;
import static lol.util.RiftKeys.REGION;
import static lol.util.RiftKeys.RICH_ENGLISH;
import static lol.util.RiftKeys.UNRANKED;

public class BannedChampionsByDayByRegion extends Configured implements Tool
{
    public final static String USAGE = "usage: out";
//    private static final String INPUT = "/user/blackburn/riftwalk.gg/riftwalk.jsons";
    private static final String INPUT = "/home/wchi/riftwalk-sample.json";

    private static final String[] REGIONS = { "ranked_rich_english", "ranked_poor_nonenglish",
                                              "unranked_rich_english", "unranked_poor_nonenglish" };

    public static void main( String[] args ) throws Exception
    {
//        if ( args.length != 1 )
//        {
//            throw new IllegalArgumentException( USAGE );
//        }

        for ( String r : REGIONS )
        {
            String[] next = new String[3];
            next[0] = args[0];
            next[1] = args[1] + r;
            next[2] = r;
            ToolRunner.run( new BannedChampionsByDayByRegion(), next );
        }
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "bannedChampionsByDayByRegion" );
        job.setJarByClass( BannedChampionsByDayByRegion.class );

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
        job.setReducerClass( ChampionsReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( Text.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        job.submit();
        return 0;
    }

    public static class RankedRichMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text DATE = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DATE.set( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
                                                       match.get( LENGTH ).toString() ) );

                Any bannedChampions = match.get( BANNED_CHAMPIONS );

                if ( bannedChampions.size() > 0 )
                {
                    for ( Any c : bannedChampions )
                    {
                        context.write( DATE, new Text( c.get( BANNED_CHAMPION_ID ).toString() ) );
                    }
                }
            }
        }
    }

    public static class RankedPoorMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text DATE = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && RANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DATE.set( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
                                                       match.get( LENGTH ).toString() ) );

                Any bannedChampions = match.get( BANNED_CHAMPIONS );

                if ( bannedChampions.size() > 0 )
                {
                    for ( Any c : bannedChampions )
                    {
                        context.write( DATE, new Text( c.get( BANNED_CHAMPION_ID ).toString() ) );
                    }
                }
            }
        }
    }

    public static class UnrankedRichMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text DATE = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DATE.set( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
                                                       match.get( LENGTH ).toString() ) );

                Any bannedChampions = match.get( BANNED_CHAMPIONS );

                if ( bannedChampions.size() > 0 )
                {
                    for ( Any c : bannedChampions )
                    {
                        context.write( DATE, new Text( c.get( BANNED_CHAMPION_ID ).toString() ) );
                    }
                }
            }
        }
    }

    public static class UnrankedPoorMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text DATE = new Text();

        @Override
        protected void map( Object key, Text json, Context context )
                throws IOException, InterruptedException
        {
            Any match = JsonIterator.deserialize( json.toString() );

            if ( POOR_NONENGLISH.contains( match.get( REGION ).toString() )
                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
            {
                DATE.set( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
                                                       match.get( LENGTH ).toString() ) );

                Any bannedChampions = match.get( BANNED_CHAMPIONS );

                if ( bannedChampions.size() > 0 )
                {
                    for ( Any c : bannedChampions )
                    {
                        context.write( DATE, new Text( c.get( BANNED_CHAMPION_ID ).toString() ) );
                    }
                }
            }
        }
    }

    public static class ChampionsReducer extends Reducer<Text, Text, Text, Text>
    {
        private Map<String, Integer> championFrequencies = new HashMap<>();

        @Override
        protected void reduce( Text date, Iterable<Text> champions, Context context )
                throws IOException, InterruptedException
        {
            for ( Text c : champions )
            {
                championFrequencies.merge( c.toString(), 1, Integer::sum );
            }

            for ( Entry<String, Integer> e : championFrequencies.entrySet() )
            {
                context.write( date, new Text(e.getKey() + "\t" + e.getValue()) );
            }
        }
    }
}
