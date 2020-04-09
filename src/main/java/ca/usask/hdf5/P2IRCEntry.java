package ca.usask.hdf5;

import ca.usask.hdf5.util.ByteBufferSerializable;
import ca.usask.hdf5.util.UnsafeUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import scala.Function1;
import scala.Unit;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Map;

public class P2IRCEntry extends ByteBufferSerializable implements Serializable, KryoSerializable {

    private static final long serialVersionUID = 4385815071458349577L;

    protected long timestamp;
    protected double lat;
    protected double lon;
    protected Map<String, Object> metadata;
    protected ByteBuffer payload;
    protected int[] dimensions;
    protected NumericType payloadType;

    // for serialization
    public P2IRCEntry() {

    }

    public P2IRCEntry(P2IRCEntry p2IRCEntry) {
        this.timestamp = p2IRCEntry.timestamp;
        this.lat = p2IRCEntry.lat;
        this.lon = p2IRCEntry.lon;
        this.dimensions = p2IRCEntry.dimensions;
        this.payloadType = p2IRCEntry.payloadType;
        this.metadata = p2IRCEntry.metadata;
        this.payload = p2IRCEntry.payload;
    }

    public P2IRCEntry(long timestamp, double lat, double lon, int[] dimensions, NumericType payloadType, Map<String, Object> metadata, ByteBuffer payload) {
        this.timestamp = timestamp;
        this.lat = lat;
        this.lon = lon;
        this.dimensions = dimensions;
        this.payloadType = payloadType;
        this.metadata = metadata;
        this.payload = payload;
    }

    public P2IRCEntry(long timestamp, double lat, double lon, int[] dimensions, NumericType payloadType, Map<String, Object> metadata, byte[] payload) {
        this.timestamp = timestamp;
        this.lat = lat;
        this.lon = lon;
        this.dimensions = dimensions;
        this.payloadType = payloadType;
        this.metadata = metadata;
        this.payload = UnsafeUtil.byteArraytoBuf(payload);
    }

    public P2IRCEntry(long timestamp, double lat, double lon, int[] dimensions, NumericType payloadType, byte[] payload) {
        this.timestamp = timestamp;
        this.lat = lat;
        this.lon = lon;
        this.dimensions = dimensions;
        this.payloadType = payloadType;
        this.payload = UnsafeUtil.byteArraytoBuf(payload);
    }

    public P2IRCEntry map(Function1<ByteBuffer, ByteBuffer> function1) {
        return new P2IRCEntry(timestamp, lat, lon, dimensions, payloadType, metadata, function1.apply(payload));
    }

    public P2IRCEntry mapMutable(Function1<ByteBuffer, Unit> function1) {
        function1.apply(payload);
        return this;
    }

    public P2IRCEntry thresholdAndMask16Bit(short index) {
        ByteBuffer buf = ByteBuffer.allocateDirect(payload.limit()).order(ByteOrder.LITTLE_ENDIAN);
        ShortBuffer nb = buf.asShortBuffer();
        ShortBuffer b = payload.asShortBuffer();
        for (int i = 0; i < b.limit(); ++i) {
            short v = b.get();
            nb.put((v > index) ? v : 0);
        }
        return new P2IRCEntry(timestamp, lat, lon, dimensions, payloadType, metadata, buf);
    }

    public Object getMetadata(String key) {
        return this.metadata.get(key);
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public byte[] getPayloadAsBytes(byte[] array) {
        UnsafeUtil.unsafe.copyMemory(null, UnsafeUtil.addressOf(payload), array, UnsafeUtil.BYTE_ARRAY_OFFSET, payload.limit());
        return array;
    }

    public short[] getPayloadAsShorts(short[] array) {
        UnsafeUtil.unsafe.copyMemory(null, UnsafeUtil.addressOf(payload), array, UnsafeUtil.SHORT_ARRAY_OFFSET, payload.limit());
        return array;
    }

    public long[] getPayloadAsLongs(long[] array) {
        UnsafeUtil.unsafe.copyMemory(null, UnsafeUtil.addressOf(payload), array, UnsafeUtil.LONG_ARRAY_OFFSET, payload.limit());
        return array;
    }

    public Map<String, Object> getMetadata() {
        return this.metadata;
    }

    final public long timestamp() {
        return timestamp;
    }

    final public double lat() {
        return lat;
    }

    final public double lon() {
        return lon;
    }

    final public int[] dimensions() {
        return dimensions;
    }

    final public NumericType payloadType() {
        return payloadType;
    }

    public double distanceTo(P2IRCEntry other) {
        double timestampDiff = timestamp - other.timestamp;
        double latDiff = lat - other.lat;
        double lonDiff = lon - other.lon;

        return Math.sqrt( (timestampDiff * timestampDiff) + (latDiff * latDiff) + (lonDiff * lonDiff) );
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();

        b.append("timestamp = ").append(timestamp);
        b.append(", latitude = ").append(lat);
        b.append(", longitude = ").append(lon);
        b.append(", dimensions = ").append(Arrays.toString(dimensions));
        b.append(", payloadType = ").append(payloadType);
        b.append(", metadata = { ");
        if (metadata != null) {
            for (Map.Entry<String, Object> e : metadata.entrySet()) {
                b.append(e.getKey()).append(" -> ").append(e.getValue());
                b.append("  ");
            }
        }
        b.append("}");

        return b.toString();
    }

    @Override
    public void readObject(ByteBuffer in) {
        timestamp = (long) read(in);
        lat = (double) read(in);
        lon = (double) read(in);
        dimensions = (int[]) read(in);
        payloadType = NumericType.values()[(int) read(in)];
        metadata = (Map<String, Object>) read(in);
        payload = (ByteBuffer) read(in);
    }

    @Override
    protected ByteBufferSerializable makeObject(ByteBuffer in) {
        P2IRCEntry e = new P2IRCEntry();
        e.readObject(in);
        return e;
    }

    @Override
    public void writeObject(ByteBuffer out) {
        write(timestamp, out);
        write(lat, out);
        write(lon, out);
        write(dimensions, out);
        write(payloadType.ordinal(), out);
        write(metadata, out);
        write(payload, out);
    }

    @Override
    public int calculateSize() {
        return calculateSize(timestamp) * 3 +
                calculateSize(dimensions) +
                calculateSize(payloadType.ordinal()) +
                calculateSize(metadata) +
                calculateSize(payload);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeObject(dimensions);
        out.writeByte(payloadType.ordinal());
        out.writeObject(metadata);
        out.writeInt(payload.limit());
        out.write(UnsafeUtil.bufToByteArray(payload));
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        lat = in.readDouble();
        lon = in.readDouble();
        dimensions = (int[]) in.readObject();
        payloadType = NumericType.values()[in.read()];
        metadata = (Map<String, Object>) in.readObject();
        int len = in.readInt();
        byte[] b = new byte[len];
        in.read(b);
        payload = UnsafeUtil.byteArraytoBuf(b);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(timestamp);
        output.writeDouble(lat);
        output.writeDouble(lon);
        output.writeByte(dimensions.length);
        output.writeInts(dimensions);
        output.writeByte(payloadType.ordinal());
        kryo.writeClassAndObject(output, metadata);
        output.writeInt(payload.limit());
        output.writeBytes(UnsafeUtil.bufToByteArray(payload));
    }

    @Override
    public void read(Kryo kryo, Input input) {
        timestamp = input.readLong();
        lat = input.readDouble();
        lon = input.readDouble();
        dimensions = input.readInts(input.readByte());
        payloadType = NumericType.values()[input.readByte()];
        metadata = (Map<String, Object>) kryo.readClassAndObject(input);
        payload = UnsafeUtil.byteArraytoBuf(input.readBytes(input.readInt()));
    }
}
