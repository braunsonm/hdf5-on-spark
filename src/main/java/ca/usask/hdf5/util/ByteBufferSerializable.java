package ca.usask.hdf5.util;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class ByteBufferSerializable {

    private enum Type {
        NULL,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        STRING,
        BYTE_BUFFER,
        MAP_STRING_OBJECT,
        INT32_ARRAY,
        SERIALIZABLE,
        SERIALIZABLE_ARRAY_1D,
        SERIALIZABLE_ARRAY_2D
    }

    abstract public void readObject(ByteBuffer in);

    abstract protected ByteBufferSerializable makeObject(ByteBuffer in);

    abstract public void writeObject(ByteBuffer out);

    abstract public int calculateSize();

    protected static void write(byte b, ByteBuffer out) {
        out.put((byte) Type.INT8.ordinal());
        out.put(b);
    }

    protected static int calculateSize(byte b) {
        return 2;
    }

    protected static void write(short s, ByteBuffer out) {
        out.put((byte) Type.INT16.ordinal());
        out.putShort(s);
    }

    protected static int calculateSize(short s) {
        return 3;
    }

    protected static void write(int i, ByteBuffer out) {
        out.put((byte) Type.INT32.ordinal());
        out.putInt(i);
    }

    protected static int calculateSize(int i) {
        return 5;
    }

    protected static void write(long l, ByteBuffer out) {
        out.put((byte) Type.INT64.ordinal());
        out.putLong(l);
    }

    protected static int calculateSize(long l) {
        return 9;
    }

    protected static void write(float f, ByteBuffer out) {
        out.put((byte) Type.FLOAT.ordinal());
        out.putFloat(f);
    }

    protected static int calculateSize(float f) {
        return 5;
    }

    protected static void write(double d, ByteBuffer out) {
        out.put((byte) Type.DOUBLE.ordinal());
        out.putDouble(d);
    }

    protected static int calculateSize(double d) {
        return 9;
    }

    protected static void write(String s, ByteBuffer out) {
        if (s == null) {
            out.put((byte) Type.NULL.ordinal());
        } else {
            out.put((byte) Type.STRING.ordinal());
            out.putShort((short) s.length());
            out.put(s.getBytes());
        }
    }

    protected static int calculateSize(String s) {
        if (s == null) {
            return 1;
        } else {
            return 3 + s.length();
        }
    }

    private static String getString(ByteBuffer in) {
        int len = in.getShort();
        byte[] b = new byte[len];
        in.get(b);
        return new String(b);
    }

    protected static void write(ByteBuffer buf, ByteBuffer out) {
        if (buf == null) {
            out.put((byte) Type.NULL.ordinal());
        } else {
            out.put((byte) Type.BYTE_BUFFER.ordinal());
            out.putInt(buf.limit());
            out.put(buf);
        }
    }

    protected static int calculateSize(ByteBuffer buf) {
        if (buf == null) {
            return 1;
        } else {
            return 5 + buf.limit();
        }
    }

    private static ByteBuffer getByteBuffer(ByteBuffer in) {
        int len = in.getInt();
        ByteBuffer b = ByteBuffer.allocateDirect(len);
        UnsafeUtil.unsafe.copyMemory(UnsafeUtil.addressOf(in) + in.position(), UnsafeUtil.addressOf(b), len);
        in.position(in.position() + len);
        return b;
    }

    protected static void write(Map<String, Object> m, ByteBuffer out) {
        if (m == null) {
            out.put((byte) Type.NULL.ordinal());
        } else {
            out.put((byte) Type.MAP_STRING_OBJECT.ordinal());
            out.putInt(m.size());
            m.forEach((key, value) -> {
                write(key, out);

                if (value instanceof Byte)              write((Byte) value, out);
                else if (value instanceof Short)        write((Short) value, out);
                else if (value instanceof Integer)      write((Integer) value, out);
                else if (value instanceof Long)         write((Long) value, out);
                else if (value instanceof Float)        write((Float) value, out);
                else if (value instanceof Double)       write((Double) value, out);
                else if (value instanceof String)       write((String) value, out);
                else if (value instanceof ByteBuffer)   write((ByteBuffer) value, out);
            });
        }
    }

    protected static int calculateSize(Map<String, Object> m) {
        if (m == null) {
            return 1;
        } else {
            final int[] total = {5};
            m.forEach((key, value) -> {
                total[0] += calculateSize(key);

                if (value instanceof Byte)              total[0] += calculateSize((Byte) value);
                else if (value instanceof Short)        total[0] += calculateSize((Short) value);
                else if (value instanceof Integer)      total[0] += calculateSize((Integer) value);
                else if (value instanceof Long)         total[0] += calculateSize((Long) value);
                else if (value instanceof Float)        total[0] += calculateSize((Float) value);
                else if (value instanceof Double)       total[0] += calculateSize((Double) value);
                else if (value instanceof String)       total[0] += calculateSize((String) value);
                else if (value instanceof ByteBuffer)   total[0] += calculateSize((ByteBuffer) value);
            });

            return total[0];
        }
    }

    private static Map<String, Object> getMapStringObject(ByteBuffer in) {
        int len = in.getInt();
        Map<String, Object> map = new HashMap<>(len);
        for (int i = 0; i < len; ++i) {
            in.get();               // skip string type
            map.put(getString(in), read(in));
        }
        return map;
    }

    protected static void write(int[] ints, ByteBuffer out) {
        if (ints == null) {
            out.put((byte) Type.NULL.ordinal());
        } else {
            out.put((byte) Type.INT32_ARRAY.ordinal());
            int len = ints.length;
            out.putInt(len);
            int pos = out.position();
            UnsafeUtil.unsafe.copyMemory(ints, UnsafeUtil.INT_ARRAY_OFFSET, null, UnsafeUtil.addressOf(out) + pos, len * 4);
            out.position(pos + len * 4);
        }
    }

    protected static int calculateSize(int[] ints) {
        if (ints == null) {
            return 1;
        } else {
            return 5 + ints.length * 4;
        }
    }

    private static int[] getIntArray(ByteBuffer in) {
        int len = in.getInt();
        int pos = in.position();
        int[] a = new int[len];
        UnsafeUtil.unsafe.copyMemory(null, UnsafeUtil.addressOf(in) + pos, a, UnsafeUtil.INT_ARRAY_OFFSET, len * 4);
        in.position(pos + len * 4);
        return a;
    }

    protected static void write(ByteBufferSerializable serializable, ByteBuffer out) {
        if (serializable == null) {
            out.put((byte) Type.NULL.ordinal());
        }
        else {
            out.put((byte) Type.SERIALIZABLE.ordinal());
            write(serializable.getClass().getCanonicalName(), out);
            serializable.writeObject(out);
        }
    }

    protected static int calculateSize(ByteBufferSerializable serializable) {
        if (serializable == null) {
            return 1;
        } else {
            return 1 + calculateSize(serializable.getClass().getCanonicalName()) + serializable.calculateSize();
        }
    }

    private static ByteBufferSerializable getSerializable(ByteBuffer in) {
        try {
            ByteBufferSerializable serializable = (ByteBufferSerializable) Class.forName(getString(in)).newInstance();
            serializable.readObject(in);
            return serializable;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
            return null;
        }
    }

    protected static void write(ByteBufferSerializable[] serializables, ByteBuffer out) {
        if (serializables == null) {
            out.put((byte) Type.NULL.ordinal());
        }
        else {
            out.put((byte) Type.SERIALIZABLE_ARRAY_1D.ordinal());
            out.putInt(serializables.length);

            for (int i = 0; i < serializables.length; ++i) {
                write(serializables[i], out);
            }
        }
    }

    protected static int calculateSize(ByteBufferSerializable[] serializables) {
        if (serializables == null) {
            return 1;
        }
        else {
            int total = 5;

            for (int i = 0; i < serializables.length; ++i) {
                total += calculateSize(serializables[i]);
            }

            return total;
        }
    }

    protected static ByteBufferSerializable[] getSerializableArray1D(ByteBuffer in, ByteBufferSerializable[] arrayTemplate) {
        int len = in.getInt();
        ByteBufferSerializable[] a = (ByteBufferSerializable[]) Array.newInstance(arrayTemplate.getClass().getComponentType(), len);

        for (int i = 0; i < len; ++i) {
            if (Type.values()[in.get()] == Type.SERIALIZABLE) {
                a[i] = getSerializable(in);
            }
            else {
                a[i] = null;
            }
        }

        return a;
    }

    protected static void write(ByteBufferSerializable[][] serializables, ByteBuffer out) {
        if (serializables == null) {
            out.put((byte) Type.NULL.ordinal());
        }
        else {
            out.put((byte) Type.SERIALIZABLE_ARRAY_2D.ordinal());
            out.putInt(serializables.length);

            for (int i = 0; i < serializables.length; ++i) {
                write(serializables[i], out);
            }
        }
    }

    protected static int calculateSize(ByteBufferSerializable[][] serializables) {
        if (serializables == null) {
            return 1;
        }
        else {
            int total = 5;

            for (int i = 0; i < serializables.length; ++i) {
                total += calculateSize(serializables[i]);
            }

            return total;
        }
    }

    protected static ByteBufferSerializable[][] getSerializableArray2D(ByteBuffer in, ByteBufferSerializable[][] arrayTemplate) {
        int len = in.getInt();
        ByteBufferSerializable[][] a = (ByteBufferSerializable[][]) Array.newInstance(arrayTemplate.getClass().getComponentType(), len);

        for (int i = 0; i < len; ++i) {
            if (Type.values()[in.get()] == Type.SERIALIZABLE_ARRAY_1D) {
                a[i] = getSerializableArray1D(in, arrayTemplate[0]);
            }
            else {
                a[i] = null;
            }
        }

        return a;
    }

    public static Object read(ByteBuffer in) {
        Type type = Type.values()[in.get()];

        switch (type) {

            case NULL:                          return null;
            case INT8:                          return in.get();
            case INT16:                         return in.getShort();
            case INT32:                         return in.getInt();
            case INT64:                         return in.getLong();
            case FLOAT:                         return in.getFloat();
            case DOUBLE:                        return in.getDouble();
            case STRING:                        return getString(in);
            case BYTE_BUFFER:                   return getByteBuffer(in);
            case MAP_STRING_OBJECT:             return getMapStringObject(in);
            case INT32_ARRAY:                   return getIntArray(in);
            case SERIALIZABLE:                  return getSerializable(in);
            case SERIALIZABLE_ARRAY_1D:         return getSerializableArray1D(in, new ByteBufferSerializable[0]);
            case SERIALIZABLE_ARRAY_2D:         return getSerializableArray2D(in, new ByteBufferSerializable[1][0]);
        }

        return null;
    }
}
