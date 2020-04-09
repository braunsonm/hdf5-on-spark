package ca.usask.hdf5.util;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class UnsafeUtil {

    public static Unsafe unsafe = null;

    public static final long BYTE_ARRAY_OFFSET;
    public static final long SHORT_ARRAY_OFFSET;
    public static final long INT_ARRAY_OFFSET;
    public static final long LONG_ARRAY_OFFSET;
    public static final long FLOAT_ARRAY_OFFSET;
    public static final long DOUBLE_ARRAY_OFFSET;

    static {
        try {
            Constructor<Unsafe> constructor = Unsafe.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            unsafe = constructor.newInstance();
        } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        assert unsafe != null;
        BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        SHORT_ARRAY_OFFSET = unsafe.arrayBaseOffset(short[].class);
        INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
        LONG_ARRAY_OFFSET = unsafe.arrayBaseOffset(long[].class);
        FLOAT_ARRAY_OFFSET = unsafe.arrayBaseOffset(float[].class);
        DOUBLE_ARRAY_OFFSET = unsafe.arrayBaseOffset(double[].class);
    }

    public static long addressOf(ByteBuffer b) {
        return ((DirectBuffer) b).address();
    }

    public static byte[] bufToByteArray(ByteBuffer b) {
        int len = b.limit();
        byte[] a = new byte[len];
        unsafe.copyMemory(null, addressOf(b), a, BYTE_ARRAY_OFFSET, len);
        return a;
    }

    public static ByteBuffer byteArraytoBuf(byte[] b) {
        int len = b.length;
        ByteBuffer buf = ByteBuffer.allocateDirect(len);
        unsafe.copyMemory(b, BYTE_ARRAY_OFFSET, null, addressOf(buf), len);
        return buf;
    }
}
