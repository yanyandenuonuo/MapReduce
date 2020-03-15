/**
 * Created by linfeng on 2020/01/01.
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MapValueOutputWritable implements Writable {
    private String val1;
    private String val2;

    public String getVal1() {
        return val1;
    }

    public String getVal2() {
        return val2;
    }

    public MapValueOutputWritable() {

    }

    public MapValueOutputWritable(String val1, String val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.val1 = dataInput.readUTF();
        this.val2 = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.val1);
        dataOutput.writeUTF(this.val2);
    }

    @Override
    public String toString() {
        return this.val1 + "\t" + this.val2;
    }

}
