package com.github.owamns.diabetesanalyticsapi.dto;

import java.util.ArrayList;
import java.util.List;

public class HadoopJobResponse {
    private boolean success;
    private String outputPath;
    private String message;
    private List<Object> results = new ArrayList<>();
    private String downloadUrl;
    private String runId;

    public HadoopJobResponse() {}

    public HadoopJobResponse(boolean success, String outputPath, String message) {
        this.success = success;
        this.outputPath = outputPath;
        this.message = message;
        // runId to be set by controller
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<Object> getResults() {
        return results;
    }

    public void setResults(List<Object> results) {
        this.results = results;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public void setDownloadUrl(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }
    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }
}
