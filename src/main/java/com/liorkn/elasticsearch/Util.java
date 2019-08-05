package com.liorkn.elasticsearch;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Base64;

/**
 * Created by Lior Knaany on 4/7/18.
 */
public class Util {

    public static float[] convertBase64ToArray(String base64Str) {
        final byte[] decode = Base64.getDecoder().decode(base64Str.getBytes());
        final FloatBuffer floatBuffer = ByteBuffer.wrap(decode).asFloatBuffer();
        final float[] dims = new float[floatBuffer.capacity()];
        floatBuffer.get(dims);

        return dims;
    }

    public static String convertArrayToBase64(float[] array) {
        final int capacity = Float.BYTES * array.length;
        final ByteBuffer bb = ByteBuffer.allocate(capacity);
        for (double v : array) {
            bb.putFloat((float) v);
        }
        bb.rewind();
        final ByteBuffer encodedBB = Base64.getEncoder().encode(bb);

        return new String(encodedBB.array());
    }

    public static String convert2dArrayToBase64(float[][] array) {
        final int capacity = Float.BYTES * array.length*array[0].length;
        final ByteBuffer bb = ByteBuffer.allocate(capacity);
        for (float[] v : array) {
            for (double num : v) {
                bb.putFloat((float) num);
            }

        }
        bb.rewind();
        final ByteBuffer encodedBB = Base64.getEncoder().encode(bb);

        return new String(encodedBB.array());
    }

    public static float[][] convertBase64To2DArray(String base64Str, int rows, int cols){
        float[] as1DArr = convertBase64ToArray(base64Str);
        float[][] as2DArr = new float[rows][cols];
        for(int i = 0; i < rows; i++){
            as2DArr[i] = Arrays.copyOfRange(as1DArr, i*(cols), (i+1)*cols);
        }

        return as2DArr;
    }
}
