/*
Based on: https://discuss.elastic.co/t/vector-scoring/85227/4
and https://github.com/MLnick/elasticsearch-vector-scoring

another slower implementation using strings: https://github.com/ginobefun/elasticsearch-feature-vector-scoring

storing arrays is no luck - lucine index doesn't keep the array members orders
https://www.elastic.co/guide/en/elasticsearch/guide/current/complex-core-fields.html

Delimited Payload Token Filter: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/analysis-delimited-payload-tokenfilter.html
 */

package com.liorkn.elasticsearch.script;

import com.liorkn.elasticsearch.Util;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
//import org.apache.commons.lang.ArrayUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Collections;


/**
 * Script that scores documents based on cosine similarity embedding vectors.
 */
public final class VectorScoreScript implements LeafSearchScript, ExecutableScript {

    // the field containing the vectors to be scored against
    public final String field;

    private int docId;
    private BinaryDocValues binaryEmbeddingReader;

//    private final float[][] inputVector = new float[][];
    private ArrayList<float[]> inputVector = new ArrayList<>();
    private ArrayList<Float> magnitudes = new ArrayList<>();

    private final boolean cosine;

    private int rows;
    private int cols;

    @Override
    public final Object run() {
        return runAsDouble();
    }

    @Override
    public long runAsLong() {
        return (long) runAsDouble();
    }

    /**
     * Called for each document
     * @return cosine similarity of the current document against the input inputVector
     */
    @Override
    public double runAsDouble() {
        System.out.println("Running runAsDouble");
        final byte[] bytes = binaryEmbeddingReader.get(docId).bytes;
        final ByteArrayDataInput input = new ByteArrayDataInput(bytes);

        // MUST appear hear since it affect the next calls
        input.readVInt(); // returns the number of values which should be 1
        input.readVInt(); // returns the number of bytes to read


        ArrayList<Double> distances = new ArrayList<>();

        if(cosine) {
            float docVectorNorm = 0.0f;
            //TODO: fix magnitude. What is it? Where does it get set?
            for (int i = 0; i < inputVector.size(); i++) {
                float magnitude = magnitudes.get(i);
                float dotprod = 0; //cosine distance

                for (int j=0; j < inputVector.get(i).length; j++) {
                    float v = Float.intBitsToFloat(input.readInt()); //double check that this gets the right values (does a next)
                    docVectorNorm += v * v;  // inputVector norm
                    dotprod += v * inputVector.get(i)[j];  // dot product
                }

                if (docVectorNorm == 0 || magnitude == 0) {
                    distances.add(0d);
                } else {
                    double cosine_d =  dotprod / (Math.sqrt(docVectorNorm) * magnitude);
                    distances.add(cosine_d);
                }

            }
            // get the final score by taking the arg max over the cosine distance of attention layer comparisons.
            // This is just the maximum over the distances vector
            double score = Collections.max(distances);
            return score;

        } else {
            float score = 0;
            for (int i = 0; i < inputVector.size(); i++) {
                for (int j=0; j < inputVector.get(i).length; j++) {
                    float v = Float.intBitsToFloat(input.readInt());
                    score += v * inputVector.get(i)[j];  // dot product
                }
            }


            return score;
        }
    }

    @Override
    public void setNextVar(String name, Object value) {}

    @Override
    public void setDocument(int docId) {
        this.docId = docId;
    }

    public void setBinaryEmbeddingReader(BinaryDocValues binaryEmbeddingReader) {
        if(binaryEmbeddingReader == null) {
            throw new IllegalStateException("binaryEmbeddingReader can't be null");
        }
        this.binaryEmbeddingReader = binaryEmbeddingReader;
    }

    /**
     * Factory that is registered in
     * {@link VectorScoringPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory {

        /**
         * This method is called for every search on every shard.
         * 
         * @param params
         *            list of script parameters passed with the query
         * @return new native script
         */
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new VectorScoreScript(params);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * Init
     * @param params index that a scored are placed in this parameter. Initialize them here.
     */
    @SuppressWarnings("unchecked")
    public VectorScoreScript(Map<String, Object> params) {
        System.out.println("running VectorScoreScript");
        final Object cosineBool = params.get("cosine");
        cosine = cosineBool != null ?
                (boolean)cosineBool :
                true;

        System.out.println("The value of cosine is " + cosine);
        final Object field = params.get("field");
        if (field == null)
            throw new IllegalArgumentException("binary_vector_score script requires field input");
        this.field = field.toString();

        // get query inputVector - convert to primitive
        //I think this may be implemented somewhere else, but I'm not sure and this might mess up somewhere
        final Object vector = params.get("vector");
        System.out.println("Vector looks like");
        System.out.println(vector);
        System.out.println(vector.getClass().getName());
        if(vector != null) {
            //TODO: now make this a 2D array
//            inputVector = (ArrayList<float[]>) vector;
//            System.out.println("inputVector looks like");
//            System.out.println(inputVector);
            final ArrayList<Float[]> tmp = (ArrayList<Float[]>) vector;
//            is this even needed?
            for (int i = 0; i < tmp.size(); i++) {
                int size = tmp.get(i).length;
                float[] v = new float[size];
                for (int j = 0; j < tmp.get(i).length; j++) {
                    v[j] = tmp.get(i)[j].floatValue();
                }
                inputVector.add(v);
            }
            System.out.println("inputVector looks like "+inputVector);
        } else {
            final Object encodedVector = params.get("encoded_vector");
            if(encodedVector == null) {
                throw new IllegalArgumentException("Must have at 'vector' or 'encoded_vector' as a parameter");
            }
            float[] tmpVector = Util.convertBase64ToArray((String) encodedVector);
            //unflatten, for now hard coded, but this shouldn't be in the future.
            int n = 10;
            int len = tmpVector.length / n;
            ArrayList<float[]> inputVector = new ArrayList<>();
            for (int i=0; i < 10; i++){
                float[] tmp = Arrays.copyOfRange(tmpVector, i*(len +1), (i+1)*len);
                inputVector.add(tmp);
            }
        }

        if(cosine) {

            // compute query inputVector norm once
            System.out.println("Computing input vector norm. inputVector: "+ inputVector);
            System.out.println(inputVector.getClass().getName());
            inputVector.forEach((v) -> {
                System.out.println(v);

            });
//            for (int i=0; i < inputVector.size(); i++) {
//                System.out.println("inside mag for loop");
//                // calc magnitude
//                float queryVectorNorm = 0.0f;
//                System.out.println("v is " + inputVector.get(i));
//                System.out.println("v type is " +inputVector.get(i).getClass().getName());
//                System.out.println("entry type is "+inputVector.get(i)[0]);
////                float[] v = inputVector.get(i);
//                for(int j=0; j<inputVector.get(i).length; j++){
//                    System.out.println(("inner for loop"));
//                    System.out.println("num is "+ inputVector.get(i)[j]);
//                    queryVectorNorm += inputVector.get(i)[j] * inputVector.get(i)[j];
//                }
//                float magnitude = (float) Math.sqrt(queryVectorNorm);
//                magnitudes.add(magnitude);
//            }

        } else {
            magnitudes.add(0.0f);
        }
        System.out.println("magnitudes looks like " + magnitudes);


    }
}