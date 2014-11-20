package org.zenoss.zep.utils;

import java.io.*;

public class SerializationUtils {

    /** No public constructors. */
    private SerializationUtils(){}

    public static byte[] serialized(final Object item) throws IOException {
        if (item == null) throw new NullPointerException();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(item);
            oos.flush();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (oos != null) oos.close();
            } catch (IOException e) {
                // eat it.
            }
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public static Object deserialized(final byte[] data) throws IOException, ClassNotFoundException {
        if (data == null || data.length == 0) return null;
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (IOException e) {
            throw e;
        } catch (ClassNotFoundException e) {
            throw e;
        } finally {
            try {
                if (ois != null) ois.close();
            } catch (IOException e) {
                // eat it.
            }
        }
    }

}
