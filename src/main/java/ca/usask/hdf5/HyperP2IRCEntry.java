package ca.usask.hdf5;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HyperP2IRCEntry extends P2IRCEntry {

    private static final long serialVersionUID = -8024212110970124628L;

    private P2IRCEntry p;
    private P2IRCEntry[][] associatedPoints;

    public HyperP2IRCEntry(P2IRCEntry p, P2IRCEntry[][] associatedPoints) {
        super(p);
        this.p = p;
        this.associatedPoints = associatedPoints;
    }

    public P2IRCEntry p() {
        return p;
    }

    public P2IRCEntry[][] associatedPoints() {
        return associatedPoints;
    }

    @Override
    public void readObject(ByteBuffer in) {
        super.readObject(in);
        in.get();           // skip type byte
        associatedPoints = (P2IRCEntry[][]) getSerializableArray2D(in, new P2IRCEntry[1][0]);
    }

    @Override
    public void writeObject(ByteBuffer out) {
        super.writeObject(out);
        write(associatedPoints, out);
    }

    @Override
    public int calculateSize() {
        return super.calculateSize() + calculateSize((associatedPoints));
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(p);
        out.writeObject(associatedPoints);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        p = (P2IRCEntry) in.readObject();
        associatedPoints = (P2IRCEntry[][]) in.readObject();

        timestamp = p.timestamp;
        lat = p.lat;
        lon = p.lon;
        metadata = p.metadata;
        payload = p.payload;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.write(kryo, output);

        output.writeInt(associatedPoints.length);

        for (int i = 0; i < associatedPoints.length; ++i) {
            output.writeInt(associatedPoints[i].length);

            for (int j = 0; j < associatedPoints[i].length; ++j) {
                associatedPoints[i][j].write(kryo, output);
            }
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.read(kryo, input);

        associatedPoints = new P2IRCEntry[input.readInt()][];

        for (int i = 0; i < associatedPoints.length; ++i) {
            associatedPoints[i] = new P2IRCEntry[input.readInt()];

            for (int j = 0; j < associatedPoints[i].length; ++j) {
                associatedPoints[i][j] = new P2IRCEntry();
                associatedPoints[i][j].read(kryo, input);
            }
        }
    }
}
