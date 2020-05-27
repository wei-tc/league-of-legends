package lol.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// TODO probably don't need this. can be removed.
public class ChampionComposite implements Writable
{
    Text champion;
    VIntWritable frequency;

    public ChampionComposite( Text champion, VIntWritable frequency )
    {
        this.champion = champion;
        this.frequency = frequency;
    }

    public ChampionComposite( String champion, int frequency )
    {
        this.champion = new Text( champion );
        this.frequency = new VIntWritable( frequency );
    }

    @Override
    public void write( DataOutput dataOutput ) throws IOException
    {
        champion.write( dataOutput );
        frequency.write( dataOutput );
    }

    @Override
    public void readFields( DataInput dataInput ) throws IOException
    {
        champion.readFields( dataInput );
        frequency.readFields( dataInput );
    }

    @Override
    public String toString()
    {
        return champion + "\t"
               + frequency;
    }
}
