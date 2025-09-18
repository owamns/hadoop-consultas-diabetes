package com.github.owamns.diabetesanalyticsapi.config;

public final class AppConstants {
    private AppConstants() { }

    public static final String DATASET_PATH = "input/datos.csv";

    public static final String HADOOP_OUTPUT_BASE = "output";

    public static final String DOWNLOAD_BASE_URL = "files";

    public static final int MAX_INLINE_RESULTS = 1000;

    // Allowed frontend origin for CORS
    public static final String FRONTEND_ORIGIN = "http://localhost:5173";

}
