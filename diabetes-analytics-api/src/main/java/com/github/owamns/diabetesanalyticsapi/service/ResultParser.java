package com.github.owamns.diabetesanalyticsapi.service;

import java.util.*;
import java.util.stream.Collectors;

public class ResultParser {
    private static final String[] RECORD_HEADERS = {
        "FECHA_CORTE","DEPARTAMENTO","PROVINCIA","DISTRITO","UBIGEO","RED","IPRESS",
        "ID_PACIENTE","EDAD_PACIENTE","SEXO_PACIENTE","EDAD_MEDICO","ID_MEDICO",
        "COD_DIAG","DIAGNOSTICO","AREA_HOSPITALARIA","SERVICIO_HOSPITALARIO",
        "ACTIVIDAD_HOSPITALARIA","FECHA_MUESTRA","FEC_RESULTADO_1","PROCEDIMIENTO_1",
        "RESULTADO_1","UNIDADES_1","FEC_RESULTADO_2","PROCEDIMIENTO_2",
        "RESULTADO_2","UNIDADES_2"
    };

    public static List<Object> parseEdadPromedio(List<String> lines) {
        return lines.stream().map(line -> {
            String[] parts = line.split("\t");
            Map<String, Object> m = new HashMap<>();
            m.put("diagnostico", parts[0]);
            m.put("averageAge", Double.parseDouble(parts[1]));
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parsePacientesPorDeptoSexo(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t");
            String[] key = kv[0].split(";");
            Map<String, Object> m = new HashMap<>();
            m.put("departamento", key[0]);
            m.put("sexo", key[1]);
            m.put("count", Long.parseLong(kv[1]));
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseProcedimientosAreaServicio(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t");
            String[] key = kv[0].split(";");
            Map<String, Object> m = new HashMap<>();
            m.put("area", key[0]);
            m.put("servicio", key[1]);
            m.put("count", Long.parseLong(kv[1]));
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseEstadisticasColesterol(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t", 2);
            Map<String, Object> m = new HashMap<>();
            m.put("metric", kv[0]);
            m.put("description", kv.length > 1 ? kv[1] : "");
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseRegistroFull(List<String> lines) {
        return lines.stream().map(line -> {
            String[] cols = line.split(";");
            Map<String, Object> m = new HashMap<>();
            for (int i = 0; i < Math.min(cols.length, RECORD_HEADERS.length); i++) {
                m.put(RECORD_HEADERS[i], cols[i]);
            }
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseMinMaxColesterol(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t", 2);
            Map<String, Object> m = new HashMap<>();
            m.put("departamento", kv[0]);
            // full description "Min: x, Max: y"
            String desc = kv.length > 1 ? kv[1] : "";
            m.put("description", desc.trim());
            return m;
        }).collect(Collectors.toList());
    }
    
    public static List<Object> parseGlucosaSobrePromedio(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t", 2);
            Map<String, Object> m = new HashMap<>();
            m.put("departamento", kv[0]);
            String desc = kv.length > 1 ? kv[1] : "";
            m.put("description", desc.trim());
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseClasificacionRiesgo(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t", 2);
            Map<String, Object> m = new HashMap<>();
            m.put("category", kv[0]);
            // parse average age as double
            if (kv.length > 1) {
                try {
                    double avg = Double.parseDouble(kv[1]);
                    m.put("avgAge", avg);
                } catch (NumberFormatException e) {
                    // fallback: include raw description
                    m.put("description", kv[1]);
                }
            }
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parsePrediccionReingreso(List<String> lines) {
        return lines.stream().map(line -> {
            String[] kv = line.split("\t");
            Map<String, Object> m = new HashMap<>();
            m.put("key", kv[0]);
            m.put("count", Long.parseLong(kv[1]));
            return m;
        }).collect(Collectors.toList());
    }

    public static List<Object> parseNormalizacionMinMax(List<String> lines) {
        // Parse full semicolon-separated records with normalized cholesterol at end
        return lines.stream()
                .filter(line -> line.contains(";"))
                .map(line -> {
                    String[] cols = line.split(";");
                    Map<String, Object> m = new HashMap<>();
                    // map each original field
                    for (int i = 0; i < RECORD_HEADERS.length && i < cols.length; i++) {
                        m.put(RECORD_HEADERS[i], cols[i]);
                    }
                    // add normalized cholesterol value if present
                    if (cols.length > RECORD_HEADERS.length) {
                        m.put("COLESTEROL_NORMALIZADO", cols[cols.length - 1]);
                    }
                    return m;
                })
                .collect(Collectors.toList());
    }
}
