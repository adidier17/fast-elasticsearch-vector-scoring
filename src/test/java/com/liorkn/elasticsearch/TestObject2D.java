package com.liorkn.elasticsearch;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Created by Lior Knaany on 4/7/18.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class TestObject2D {
    int jobId;
    String embeddingVector;
    float[][] vector;
    int rows;
    int cols;

    public int getJobId() {
        return jobId;
    }

    public String getEmbeddingVector() {
        return embeddingVector;
    }

    public float[][] getVector() {
        return vector;
    }

    public TestObject2D(int jobId, float[][] vector, int rows, int cols) {
        this.jobId = jobId;
        this.vector = vector;
        this.embeddingVector = Util.convert2dArrayToBase64(vector);
        this.rows = rows;
        this.cols = cols;
    }
}
