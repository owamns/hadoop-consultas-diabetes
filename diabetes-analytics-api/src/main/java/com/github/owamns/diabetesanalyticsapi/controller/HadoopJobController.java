package com.github.owamns.diabetesanalyticsapi.controller;

import com.github.owamns.diabetesanalyticsapi.dto.HadoopJobRequest;
import com.github.owamns.diabetesanalyticsapi.dto.HadoopJobResponse;
import com.github.owamns.diabetesanalyticsapi.service.HadoopJobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.UUID;

@RestController
@RequestMapping("/api/hadoop")
@Tag(name = "Hadoop Jobs", description = "Endpoints for launching Hadoop MapReduce jobs")
public class HadoopJobController {

    private final HadoopJobService hadoopJobService;

    public HadoopJobController(HadoopJobService hadoopJobService) {
        this.hadoopJobService = hadoopJobService;
    }

    @PostMapping("/edad-promedio")
    @Operation(summary = "Run average age by diagnosis job")
    public ResponseEntity<HadoopJobResponse> runEdadPromedio(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runEdadPromedio(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/pacientes-depto-sexo")
    @Operation(summary = "Run patients count by department and sex job")
    public ResponseEntity<HadoopJobResponse> runPacientesPorDeptoSexo(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runPacientesPorDeptoSexo(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/procedimientos-area-servicio")
    @Operation(summary = "Run procedures count by area and service job")
    public ResponseEntity<HadoopJobResponse> runProcedimientosPorAreaServicio(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runProcedimientosPorAreaServicio(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/estadisticas-colesterol")
    @Operation(summary = "Run cholesterol descriptive statistics job")
    public ResponseEntity<HadoopJobResponse> runEstadisticasColesterol(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runEstadisticasColesterol(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/busqueda-subtexto")
    @Operation(summary = "Run substring search job")
    public ResponseEntity<HadoopJobResponse> runBusquedaSubtexto(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runBusquedaSubtexto(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/busqueda-fechas")
    @Operation(summary = "Run date range search job")
    public ResponseEntity<HadoopJobResponse> runBusquedaPorFechas(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runBusquedaPorFechas(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/min-max-colesterol")
    @Operation(summary = "Run min-max cholesterol by department job")
    public ResponseEntity<HadoopJobResponse> runMinMaxColesterol(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runMinMaxColesterol(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/glucosa-sobre-promedio")
    @Operation(summary = "Run glucose above national average job")
    public ResponseEntity<HadoopJobResponse> runGlucosaSobrePromedio(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runGlucosaSobrePromedio(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/clasificacion-riesgo")
    @Operation(summary = "Run cardiovascular risk classification job")
    public ResponseEntity<HadoopJobResponse> runClasificacionRiesgo(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runClasificacionRiesgo(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/prediccion-reingreso")
    @Operation(summary = "Run simple readmission prediction job")
    public ResponseEntity<HadoopJobResponse> runPrediccionReingreso(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runPrediccionReingreso(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/normalizacion-minmax-colesterol")
    @Operation(summary = "Run min-max normalization of cholesterol by province job")
    public ResponseEntity<HadoopJobResponse> runNormalizacionMinMax(@Valid @RequestBody HadoopJobRequest request) {
        String runId = UUID.randomUUID().toString();
        request.setRunId(runId);
        HadoopJobResponse response = hadoopJobService.runNormalizacionMinMax(request);
        response.setRunId(runId);
        return ResponseEntity.ok(response);
    }
}
