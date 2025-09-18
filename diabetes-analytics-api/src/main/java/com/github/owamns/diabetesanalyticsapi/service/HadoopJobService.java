package com.github.owamns.diabetesanalyticsapi.service;

import com.github.owamns.diabetesanalyticsapi.dto.HadoopJobRequest;
import com.github.owamns.diabetesanalyticsapi.dto.HadoopJobResponse;
import hadoop.q1_consultas_multiples_campos.EdadPromedioPorDiagnostico;
import hadoop.q1_consultas_multiples_campos.PacientesPorDeptoSexo;
import hadoop.q1_consultas_multiples_campos.ProcedimientosPorAreaServicio;
import hadoop.q2_estadisticas_descriptivas.EstadisticasColesterol;
import hadoop.q3_busqueda_subtexto.BusquedaSubtexto;
import hadoop.q4_busqueda_rango_fechas.BusquedaPorFechas;
import hadoop.q5_valores_extremos.MinMaxColesterolPorDepto;
import hadoop.q6_jobs_encadenados.GlucosaSobrePromedioNacional;
import hadoop.q6_jobs_encadenados.NormalizacionMinMaxColesterol;
import hadoop.q7_modelos_clasificacion.ClasificacionRiesgoCardiovascular;
import hadoop.q7_modelos_clasificacion.PrediccionReingresoSimple;
import com.github.owamns.diabetesanalyticsapi.config.AppConstants;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.github.owamns.diabetesanalyticsapi.service.ResultParser;

@Service
public class HadoopJobService {

    // Helper to read output files (part-*) and collect lines
    private List<String> readJobOutput(String outputPath) throws IOException {
        List<String> results = new ArrayList<>();
        Path dir = Paths.get(outputPath);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "part-*") ) {
            for (Path file : stream) {
                results.addAll(Files.readAllLines(file));
            }
        }
        return results;
    }

    // add helper
    private String buildDownloadUrl(String outputPath) {
        // strip base 'output/' prefix
        String rel = outputPath.startsWith(AppConstants.HADOOP_OUTPUT_BASE + "/")
            ? outputPath.substring(AppConstants.HADOOP_OUTPUT_BASE.length() + 1)
            : outputPath;
        return AppConstants.DOWNLOAD_BASE_URL + "/" + rel;
    }

    public HadoopJobResponse runEdadPromedio(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/edad-promedio";
            boolean success = EdadPromedioPorDiagnostico.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseEdadPromedio(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/edad-promedio";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runPacientesPorDeptoSexo(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/pacientes-depto-sexo";
            boolean success = PacientesPorDeptoSexo.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parsePacientesPorDeptoSexo(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/pacientes-depto-sexo";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runProcedimientosPorAreaServicio(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/procedimientos-area-servicio";
            boolean success = ProcedimientosPorAreaServicio.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseProcedimientosAreaServicio(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/procedimientos-area-servicio";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runEstadisticasColesterol(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/estadisticas-colesterol";
            boolean success = EstadisticasColesterol.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseEstadisticasColesterol(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/estadisticas-colesterol";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runBusquedaSubtexto(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/busqueda-subtexto";
            boolean success = BusquedaSubtexto.runJob(inputPath, outputPath, request.getSearchTerm());
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseRegistroFull(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/busqueda-subtexto";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runBusquedaPorFechas(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/busqueda-fechas";
            boolean success = BusquedaPorFechas.runJob(inputPath, outputPath, request.getStartDate(), request.getEndDate());
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseRegistroFull(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/busqueda-fechas";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runMinMaxColesterol(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/min-max-colesterol";
            boolean success = MinMaxColesterolPorDepto.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseMinMaxColesterol(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/min-max-colesterol";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runGlucosaSobrePromedio(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/glucosa-sobre-promedio";
            boolean success = GlucosaSobrePromedioNacional.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseGlucosaSobrePromedio(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/glucosa-sobre-promedio";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runClasificacionRiesgo(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/clasificacion-riesgo";
            boolean success = ClasificacionRiesgoCardiovascular.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseClasificacionRiesgo(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/clasificacion-riesgo";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runPrediccionReingreso(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/prediccion-reingreso";
            boolean success = PrediccionReingresoSimple.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parsePrediccionReingreso(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/prediccion-reingreso";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }

    public HadoopJobResponse runNormalizacionMinMax(HadoopJobRequest request) {
        try {
            String inputPath = AppConstants.DATASET_PATH;
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/normalizacion-minmax-colesterol";
            boolean success = NormalizacionMinMaxColesterol.runJob(inputPath, outputPath);
            String msg = success ? "Job completed successfully." : "Job failed to complete.";
            HadoopJobResponse response = new HadoopJobResponse(success, outputPath, msg);
            if (success) {
                List<String> all = readJobOutput(outputPath);
                List<Object> parsed = ResultParser.parseNormalizacionMinMax(all);
                if (parsed.size() <= AppConstants.MAX_INLINE_RESULTS) {
                    response.setResults(parsed);
                } else {
                    response.setResults(parsed.subList(0, AppConstants.MAX_INLINE_RESULTS));
                    response.setDownloadUrl(buildDownloadUrl(outputPath));
                }
            }
            return response;
        } catch (Exception e) {
            String outputPath = AppConstants.HADOOP_OUTPUT_BASE + "/" + request.getRunId() + "/normalizacion-minmax-colesterol";
            return new HadoopJobResponse(false, outputPath, e.getMessage());
        }
    }
}
