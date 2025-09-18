package com.github.owamns.diabetesanalyticsapi.controller;

import com.github.owamns.diabetesanalyticsapi.config.AppConstants;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@RestController
@RequestMapping("/api/hadoop")
public class DownloadController {

    @GetMapping("/download/{runId}/{jobName}")
    public ResponseEntity<Resource> downloadJobOutput(
            @PathVariable String runId,
            @PathVariable String jobName
    ) {
        Path dir = Paths.get(AppConstants.HADOOP_OUTPUT_BASE, runId, jobName);
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return ResponseEntity.notFound().build();
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(baos)) {

            try (java.util.stream.Stream<Path> paths = Files.walk(dir)) {
                paths.filter(Files::isRegularFile)
                     .forEach(path -> {
                    try {
                        ZipEntry entry = new ZipEntry(dir.relativize(path).toString());
                        zos.putNextEntry(entry);
                        zos.write(Files.readAllBytes(path));
                        zos.closeEntry();
                    } catch (IOException ignored) {}
                });
            }
            // Finish the ZIP stream to write central directory before extracting bytes
            zos.finish();
            // Capture ZIP bytes before deleting the directory
            byte[] zipBytes = baos.toByteArray();
            // Delete the job output directory and its contents
            try (java.util.stream.Stream<Path> deletePaths = Files.walk(dir)) {
                deletePaths.sorted(java.util.Comparator.reverseOrder())
                           .forEach(pathToDelete -> {
                               try { Files.delete(pathToDelete); } catch (IOException ignored) {}
                           });
            } catch (IOException ignored) {}
            // Delete the job directory itself
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ignored) {}
            // Also delete the runId directory if empty
            try {
                Files.deleteIfExists(dir.getParent());
            } catch (IOException ignored) {}
            // Create resource from ZIP bytes
            ByteArrayResource resource = new ByteArrayResource(zipBytes);
            String filename = jobName + ".zip";
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }
}
