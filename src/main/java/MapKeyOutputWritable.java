/**
 * Created by linfeng on 2020/01/01.
 */

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MapKeyOutputWritable implements WritableComparable<MapKeyOutputWritable> {
    private String key1;
    private String key2;

    public String getKey1() {
        return key1;
    }

    public String getKey2() {
        return key2;
    }

    public MapKeyOutputWritable() {

    }
    
    public MapKeyOutputWritable(String key1, String key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    public void set(String key1, String key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key1 = dataInput.readUTF();
        this.key2 = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.key1);
        dataOutput.writeUTF(this.key2);
    }

    @Override
    public String toString() {
        return this.key1 + "\t" + this.key2;
    }

    @Override
    public int compareTo(MapKeyOutputWritable key) {
        if (this.key1.equals(key.getKey1())) {
            return this.key2.compareTo(key.getKey2());
        } else {
            return this.key1.compareTo(key.getKey1());
        }
    }

    @Override
    public int hashCode() {
        String urlCodeString = this.key1 + "\t" + this.key2;
        return urlCodeString.hashCode();
    }
}
