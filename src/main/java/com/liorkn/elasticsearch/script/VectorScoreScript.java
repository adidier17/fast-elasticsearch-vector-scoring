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


//    private ArrayList<float[]> inputVector = new ArrayList<>();
    private ArrayList<Float> magnitudes = new ArrayList<>();

    private final boolean cosine;

    private int rows;
    private int cols;
    private float[][] inputVector = new float[rows][cols];
//    private float[] magnitudes = new float[rows];

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
        System.out.println("doc id is "+docId);
        final byte[] bytes = binaryEmbeddingReader.get(docId).bytes;
        final ByteArrayDataInput input = new ByteArrayDataInput(bytes);

        // MUST appear hear since it affect the next calls
        input.readVInt(); // returns the number of values which should be 1
        input.readVInt(); // returns the number of bytes to read


        ArrayList<Double> distances = new ArrayList<>();

        if(this.cosine) {
            float docVectorNorm = 0.0f;
            System.out.println("Computing distance");
            for (int i = 0; i < this.rows; i++) {
                System.out.println("i "+ i);
                float magnitude = magnitudes.get(i);
                float dotprod = 0; //cosine distance
                System.out.println("magnitude "+ magnitude);
                for (int j=0; j < this.cols; j++) {
                    System.out.println("j "+j);
                    float v = Float.intBitsToFloat(input.readInt()); //double check that this gets the right values (does a next)
                    System.out.println(v);
                    docVectorNorm += v * v;  // inputVector norm
                    dotprod += v * this.inputVector[i][j];  // dot product
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
            for (int i = 0; i < this.inputVector.length; i++) {
                for (int j=0; j < this.inputVector[i].length; j++) {
                    float v = Float.intBitsToFloat(input.readInt());
                    score += v * this.inputVector[i][j];  // dot product
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
        this.cosine = cosineBool != null ?
                (boolean)cosineBool :
                true;

        System.out.println("The value of cosine is " + cosine);

        Object rows_tmp = params.get("rows");
        Object cols_tmp = params.get("cols");
        this.rows = (Integer) rows_tmp;
        this.cols = (Integer) cols_tmp;


        final Object field = params.get("field");
        if (field == null)
            throw new IllegalArgumentException("binary_vector_score script requires field input");
        this.field = field.toString();

        // get query inputVector - convert to primitive
        //I think this may be implemented somewhere else, but I'm not sure and this might mess up somewhere
        final Object vector = params.get("vector");
        if(vector != null) {

            final ArrayList<Float[]> tmp = (ArrayList<Float[]>) vector;
            System.out.println("tmp looks like " + tmp);
            System.out.println("tmp is " + tmp.getClass());
            for (int i = 0; i < tmp.size(); i++) {
                int size = tmp.get(i).length;
                float[] v = new float[size];
                for (int j = 0; j < tmp.get(i).length; j++) {
                    v[j] = tmp.get(i)[j].floatValue();
                }
                inputVector[i] = v;
            }
            System.out.println("inputVector looks like "+inputVector);
        } else {
            final Object encodedVector = params.get("encoded_vector");
            if(encodedVector == null) {
                throw new IllegalArgumentException("Must have at 'vector' or 'encoded_vector' as a parameter");
            }

            inputVector= Util.convertBase64To2DArray((String) encodedVector, rows, cols);
            System.out.println("inputVector looks like! " + Arrays.deepToString(inputVector));

        }

        if(cosine) {

            // compute query inputVector norm once
//            System.out.println("Computing input vector norm. inputVector: "+ Arrays.deepToString(inputVector));
            for (int i=0; i < rows; i++) {
//                System.out.println("inside mag for loop");
                // calc magnitude
                float queryVectorNorm = 0.0f;
                for(int j=0; j<cols; j++){
//                    System.out.println("j "+j);
//                    System.out.println(inputVector[i][j]);
                    queryVectorNorm += inputVector[i][j] * inputVector[i][j];
                }
                float magnitude = (float) Math.sqrt(queryVectorNorm);
//                System.out.println(magnitude);
                magnitudes.add(magnitude);

            }

        } else {
            for (int i=0; i < rows; i++)
            magnitudes.add(0.0f);
        }
        System.out.println("magnitudes looks like " + magnitudes);


    }
}