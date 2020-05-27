package lol.util;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static lol.util.RiftKeys.BANNED_CHAMPIONS;
import static lol.util.RiftKeys.BANNED_CHAMPION_ID;
import static lol.util.RiftKeys.END_TIME;
import static lol.util.RiftKeys.LENGTH;

public class TimeProcessing
{
    public final static DateTimeFormatter DTF_IN = DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss Z" );
    public final static DateTimeFormatter DTF_OUT = DateTimeFormat.forPattern( "yyyy-MM-dd" );
    public final static DateTimeFormatter TIME_OUT = DateTimeFormat.forPattern( "HH" );
    public final static Map<String, Integer> DAY_VALUE = createMap();

    private static Map<String, Integer> createMap()
    {
        Map<String, Integer> map = new HashMap<>();
        map.put( "Monday", 0 );
        map.put( "Tuesday", 1 );
        map.put( "Wednesday", 2 );
        map.put( "Thursday", 3 );
        map.put( "Friday", 4 );
        map.put( "Saturday", 5 );
        map.put( "Sunday", 6 );
        return map;
    }

    public static void main( String[] args ) throws IOException
    {
        // testing
        String filename = "riftwalk-sample.json";
        List<String> json = Files.readAllLines( Paths.get( filename ), UTF_8 );
        for ( String s : json )
        {
            Any match = JsonIterator.deserialize( s );
            System.out.println( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
                                                       match.get( LENGTH ).toString() ) );


            Any bannedChampions = match.get( BANNED_CHAMPIONS );
                System.out.println( getStartDayTime( match.get( END_TIME ).toString(),
                                             match.get( LENGTH ).toString() ) );

                if ( bannedChampions.size() > 0 )
                {
                    for ( Any c : bannedChampions )
                    {
                        System.out.println(c.get( BANNED_CHAMPION_ID ).toString());
                    }
                }
        }

////        for ( int i = 0; i < 15; i++ )
////        {
////            String c = match.get( CHAMPION_IDS ).get( i ).toString();
////            if ( !c.isEmpty() )
////            {
////                System.out.println(c);
////            }
////        }
//        Any champions = match.get( CHAMPION_IDS );
//        if ( champions.size() > 0 )
//        {
//            for ( Any c : champions )
//            {
//                System.out.println( c.toString() );
//            }
//        }




//            if ( RICH_ENGLISH.contains( match.get( REGION ).toString() )
//                 && UNRANKED.contains( match.get( QUEUE_TYPE ).toString() ) )
//            {
//                System.out.println( TimeProcessing.getStartDate( match.get( END_TIME ).toString(),
//                                                       match.get( LENGTH ).toString() ) );
//
//                for ( Any c : match.get( CHAMPION_IDS ) )
//                {
//                    System.out.println( c.toString());
//                }
//            }

////        String endTime = match.get( END_TIME ).toString();
////        String duration = match.get( LENGTH ).toString();
////        System.out.println( endTime );
////        System.out.println( duration );
////        System.out.println( getStartDate( endTime, duration ) );
////        System.out.println( convertToMinutes( duration ) );
////        System.out.println( getStartDayTime( endTime, duration ) );
    }

    public static String getStartDate( String endDateTimeZone, String gameDuration )
    {
        DateTime dateTime = DTF_IN.withOffsetParsed().parseDateTime( endDateTimeZone );
        int ms = (int) ( Double.parseDouble( gameDuration ) * 1000 );
        dateTime = dateTime.minusMillis( ms );
        return DTF_OUT.print( dateTime );
    }

    public static String getStartTime( String endDateTimeZone, String gameDuration )
    {
        DateTime dateTime = DTF_IN.withOffsetParsed().parseDateTime( endDateTimeZone );
        int ms = (int) ( Double.parseDouble( gameDuration ) * 1000 );
        dateTime = dateTime.minusMillis( ms );
        return TIME_OUT.print( dateTime );
    }

    public static String getStartDayTime( String endDateTimeZone, String gameDuration )
    {
        DateTime dateTime = DTF_IN.withOffsetParsed().parseDateTime( endDateTimeZone );
        int ms = (int) ( Double.parseDouble( gameDuration ) * 1000 );
        dateTime = dateTime.minusMillis( ms );
        int weeklyHour = Integer.parseInt( TIME_OUT.print( dateTime ) ) +
                         24 * DAY_VALUE.get( dateTime.dayOfWeek().getAsText() );
        return Integer.toString( weeklyHour );
    }

    public static int convertToMinutes( String gameDuration )
    {
        // no data loss since not that many games have been played
        return (int) Math.round( Double.parseDouble( gameDuration ) / 60 );
    }
}
