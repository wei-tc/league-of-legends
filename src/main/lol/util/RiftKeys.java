package lol.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RiftKeys
{
    public static String REGION = "region";
    public static String BR = "br";
    public static String EUNE = "eune";
    public static String EUW = "euw";
    public static String LAN = "lan";
    public static String LAS = "las";
    public static String NA = "na";
    public static String OCE = "oce";
    public static String RU = "ru";
    public static String TR = "tr";
    public static Set<String> RICH_ENGLISH = new HashSet<>( Arrays.asList( EUNE, EUW, NA, OCE ) );
    public static Set<String> POOR_NONENGLISH = new HashSet<>( Arrays.asList( BR, LAN, LAS, RU, TR ) );
    public static Set<String> RICH_ENGLISH_NA = new HashSet<>( Collections.singletonList( NA ) );
    public static Set<String> POOR_NONENGLISH_SA = new HashSet<>( Arrays.asList( BR, LAN, LAS ) );

    public static String LENGTH = "length";
    public static String END_TIME = "end_time";

    public static String MAP_ID = "map_id";
    public static String SUMMONERS_RIFT = "1";

    public static String BANNED_CHAMPIONS = "banned_champions";
    public static String BANNED_CHAMPION_ID = "championID";
    public static String CHAMPION_IDS = "champion_ids";

    public static String QUEUE_TYPE = "queue_type";
    public static Set<String> RANKED = new HashSet<>( Arrays.asList( "7" ) );

    public static Set<String> UNRANKED = new HashSet<>( Arrays.asList( "14" ) );

    public static String DATA = "data";
    public static String WARDFRAMES = "wardFrames";
    public static String WARD_START_TIME = "startTime";
    public static String WARD_END_TIME = "endTime";
    public static String X = "x";
    public static String Y = "y";
}