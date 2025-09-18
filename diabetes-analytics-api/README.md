# Diabetes Analytics API Backend

This is a Spring Boot application exposing REST endpoints that run Hadoop MapReduce jobs on a `datos.csv` dataset. It provides inline results for small output sizes and ZIP downloads for larger outputs.

## Features

- Launch multiple Hadoop MapReduce jobs via simple HTTP POST requests
- Inline JSON responses up to a configurable limit (default 1000 lines)
- ZIP download endpoint for full job output
- Static file serving for Hadoop output directory under `/files/**`
- Swagger/OpenAPI annotations for easy API exploration

## Prerequisites

- Java 8
- Maven 3.6+
- Hadoop libraries on classpath (bundled via Maven dependencies)

## Running the Backend

```bash
# Build
./mvnw clean package

# Run
java -jar target/diabetes-analytics-api-0.0.1-SNAPSHOT.jar
```

Default server: `http://localhost:8080`

## Available Endpoints

Base path: `/api/hadoop`

All POST endpoints now generate a unique `runId` on the server and return it in the JSON response. You do not need to include `runId` in the request body.

For requests that require extra parameters (`searchTerm`, `startDate`, `endDate`), include only those fields in the body.

| Path                                    | Description                                           | Body fields                    |
| --------------------------------------- | ----------------------------------------------------- | ------------------------------- |
| POST `/edad-promedio`                   | Average age by diagnosis                              | (no body)                       |
| POST `/pacientes-depto-sexo`            | Count patients by department and sex                  | (no body)                       |
| POST `/procedimientos-area-servicio`    | Count procedures by area and service                  | (no body)                       |
| POST `/estadisticas-colesterol`         | Descriptive stats for cholesterol                     | (no body)                       |
| POST `/busqueda-subtexto`               | Filter rows containing a substring                    | `searchTerm`                    |
| POST `/busqueda-fechas`                 | Filter rows by sample date range                      | `startDate`, `endDate`          |
| POST `/min-max-colesterol`              | Min/Max cholesterol by department                     | (no body)                       |
| POST `/glucosa-sobre-promedio`          | Departments above national glucose average            | (no body)                       |
| POST `/clasificacion-riesgo`            | Cardiovascular risk classification                    | (no body)                       |
| POST `/prediccion-reingreso`            | Simple readmission prediction                         | (no body)                       |
| POST `/normalizacion-minmax-colesterol` | Min-Max normalization of cholesterol by province       | (no body)                       |
| GET  `/download/{runId}/{jobName}`      | Download full job output as ZIP (all part-*)          | n/a                             |

### Response Structure

```json
{
  "success": true,
  "runId": "<generated-run-id>",
  "outputPath": "output/<runId>/<jobName>",
  "message": "Job completed successfully.",
  "results": [ /* sample lines up to limit */ ],
  "downloadUrl": "/api/hadoop/download/<runId>/<jobName>" /* when results truncated */
}
```

- **results**: Array of strings (`KEY\tVALUE` or full CSV lines)
- **downloadUrl**: Relative URL for retrieving entire output directory (ZIP or static files)

### Ejemplos de salida por endpoint

#### Edad promedio por diagnóstico (`/api/hadoop/edad-promedio`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/edad-promedio",
  "message": "Job completed successfully.",
  "results": [
    {
      "diagnostico": "DIABETES MELLITUS ASOCIADA CON DESNUTRICION, CON CETOACIDOSIS",
      "averageAge": 56.07692307692308
    },
    {
      "diagnostico": "DIABETES MELLITUS ASOCIADA CON DESNUTRICION, CON COMA",
      "averageAge": 52.25
    },
    {
      "diagnostico": "DIABETES MELLITUS ASOCIADA CON DESNUTRICION, CON COMPLICACIONES CIRCULATORIAS PERIFERICAS",
      "averageAge": 64.58666666666667
    },
    {
      "diagnostico": "DIABETES MELLITUS ASOCIADA CON DESNUTRICION, CON COMPLICACIONES MULTIPLES",
      "averageAge": 61.50622406639004
    }
  ]
}
```

#### Conteo de pacientes por departamento y sexo (`/api/hadoop/pacientes-depto-sexo`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/pacientes-depto-sexo",
  "message": "Job completed successfully.",
  "results": [
    {
      "count": 1034,
      "departamento": "AMAZONAS",
      "sexo": "FEMENINO"
    },
    {
      "count": 949,
      "departamento": "AMAZONAS",
      "sexo": "MASCULINO"
    },
    {
      "count": 12556,
      "departamento": "ANCASH",
      "sexo": "FEMENINO"
    },
    {
      "count": 8648,
      "departamento": "ANCASH",
      "sexo": "MASCULINO"
    }
  ]
}
```

#### Conteo de procedimientos por área y servicio (`/api/hadoop/procedimientos-area-servicio`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/procedimientos-area-servicio",
  "message": "Job completed successfully.",
  "results": [
    {
      "area": "CONSULTA EXTERNA",
      "servicio": "ANESTESIA, ANALGESIA Y REANIMACION",
      "count": 948
    },
    {
      "area": "CONSULTA EXTERNA",
      "servicio": "CARDIOLOGIA",
      "count": 9896
    },
    {
      "area": "CONSULTA EXTERNA",
      "servicio": "CARDIOLOGIA INVASIVA",
      "count": 2
    },
    {
      "area": "CONSULTA EXTERNA",
      "servicio": "CIRUGIA DE TORAX Y CARDIOVASCULAR",
      "count": 20
    }
  ]
}
```

#### Estadísticas descriptivas para el colesterol  (`/api/hadoop/estadisticas-colesterol`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/estadisticas-colesterol",
  "message": "Job completed successfully.",
  "results": [
    {
      "metric": "COLESTEROL_RESULTADO_1",
      "description": "Promedio: 190.60, Mediana: 189.00, Desv. Estandar: 51.55, Registros: 509716"
    }
  ]
}
```

#### Búsqueda por subtexto  (`/api/hadoop/busqueda-subtexto`)
Request body:
```json
{"searchTerm": "COMPLICACION"}
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/busqueda-subtexto",
  "message": "Job completed successfully.",
  "results": [
    {
      "FEC_RESULTADO_1": "20200103",
      "UBIGEO": "021801",
      "FEC_RESULTADO_2": "20200103",
      "DISTRITO": "CHIMBOTE",
      "EDAD_PACIENTE": "62",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "ANCASH",
      "SERVICIO_HOSPITALARIO": "MEDICINA INTERNA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "PROCEDIMIENTO_1": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "RESULTADO_1": "235.0",
      "IPRESS": "H.I CONO SUR",
      "RESULTADO_2": "281.0",
      "COD_DIAG": "E10.9",
      "FECHA_MUESTRA": "20200103",
      "RED": "RED ASISTENCIAL ANCASH",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "38",
      "PROVINCIA": "SANTA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwztLSwMDI0sLQ0MzexNDKwNAMAH+EDiw==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 1, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDc1MzczNjc2MLU0MDWyNAMAH5EDgg=="
    },
    {
      "FEC_RESULTADO_1": "20200103",
      "UBIGEO": "200101",
      "FEC_RESULTADO_2": "20200103",
      "DISTRITO": "PIURA",
      "EDAD_PACIENTE": "60",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "PIURA",
      "SERVICIO_HOSPITALARIO": "MEDICINA FAMILIAR Y COMUNITARIA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "122.9",
      "IPRESS": "CAP III METROPOLITANO DE PIURA",
      "RESULTADO_2": "236.2",
      "COD_DIAG": "E10.7",
      "FECHA_MUESTRA": "20200103",
      "RED": "RED ASISTENCIAL PIURA",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "41",
      "PROVINCIA": "PIURA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwztDQ3N7S0NDY1NTI0NTIyMQQAH8ADeg==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 1, CON COMPLICACIONES MULTIPLES",
      "ID_PACIENTE": "eJwzNDA1NjQ2MzczNTGxNDI2MAEAHtQDcA=="
    },
    {
      "FEC_RESULTADO_1": "20200103",
      "UBIGEO": "200101",
      "FEC_RESULTADO_2": "20200103",
      "DISTRITO": "PIURA",
      "EDAD_PACIENTE": "62",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "PIURA",
      "SERVICIO_HOSPITALARIO": "MEDICINA GENERAL",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "219.1",
      "IPRESS": "CAP III METROPOLITANO DE PIURA",
      "RESULTADO_2": "356.8",
      "COD_DIAG": "E10.9",
      "FECHA_MUESTRA": "20200103",
      "RED": "RED ASISTENCIAL PIURA",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "41",
      "PROVINCIA": "PIURA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzNDQ0MzQwtjAzsTS1NDexsAQAHuQDgw==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 1, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDA1MzEyNre0sDS2MDUxMQQAH1cDgA=="
    },
    {
      "FEC_RESULTADO_1": "20200104",
      "UBIGEO": "150122",
      "FEC_RESULTADO_2": "20200104",
      "DISTRITO": "MIRAFLORES",
      "EDAD_PACIENTE": "38",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "LIMA",
      "SERVICIO_HOSPITALARIO": "ENDOCRINOLOGIA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "252.0",
      "IPRESS": "H.III SUAREZ-ANGAMOS",
      "RESULTADO_2": "203.0",
      "COD_DIAG": "E10.9",
      "FECHA_MUESTRA": "20200104",
      "RED": "RED ASISTENCIAL REBAGLIATI",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "53",
      "PROVINCIA": "LIMA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzNLQ0MTA2NbI0Njc1NzGwMAEAHw8DeQ==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 1, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzMjAyNDEytDA1NzI1MTEzNQMAHo0DcQ=="
    }
  ]
}
```

#### Búsqueda por rango de fechas (`/api/hadoop/busqueda-fechas`)
Request body:
```json
{"startDate": "20200101", "endDate": "20200102"}
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/busqueda-fechas",
  "message": "Job completed successfully.",
  "results": [
    {
      "FEC_RESULTADO_1": "20200111",
      "UBIGEO": "250107",
      "FEC_RESULTADO_2": "20200111",
      "DISTRITO": "MANANTAY",
      "EDAD_PACIENTE": "51",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "UCAYALI",
      "SERVICIO_HOSPITALARIO": "MEDICINA GENERAL",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "217.0",
      "IPRESS": "CAP I MANANTAY",
      "RESULTADO_2": "258.0",
      "COD_DIAG": "E11.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL UCAYALI",
      "SEXO_PACIENTE": "MASCULINO",
      "EDAD_MEDICO": "26",
      "PROVINCIA": "CORONEL PORTILLO",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzNjA2MzE2NLY0NzO2NDWyMAQAHxMDeg==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 2, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDAwtDC0NDMxNzAyMzWxMAQAHqoDcA=="
    },
    {
      "FEC_RESULTADO_1": "20200111",
      "UBIGEO": "250105",
      "FEC_RESULTADO_2": "20200111",
      "DISTRITO": "YARINACOCHA",
      "EDAD_PACIENTE": "48",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "UCAYALI",
      "SERVICIO_HOSPITALARIO": "MEDICINA GENERAL",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "192.0",
      "IPRESS": "P.M. ALAMEDA",
      "RESULTADO_2": "171.0",
      "COD_DIAG": "E13.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL UCAYALI",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "26",
      "PROVINCIA": "CORONEL PORTILLO",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzsjS1NDI2MjE2NTaxNLEwsAQAH4YDgg==",
      "DIAGNOSTICO": "DIABETES MELLITUS ESPECIFICADA, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDAwMjAxNTUxsbQ0NTAzMgUAHnQDbg=="
    },
    {
      "FEC_RESULTADO_1": "20200102",
      "UBIGEO": "230103",
      "FEC_RESULTADO_2": "20200102",
      "DISTRITO": "CALANA",
      "EDAD_PACIENTE": "67",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "TACNA",
      "SERVICIO_HOSPITALARIO": "ENDOCRINOLOGIA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "203.0",
      "IPRESS": "H.III DANIEL ALCIDES CARRION",
      "RESULTADO_2": "78.0",
      "COD_DIAG": "E11.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL TACNA",
      "SEXO_PACIENTE": "MASCULINO",
      "EDAD_MEDICO": "38",
      "PROVINCIA": "TACNA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwztLQwNjM1NTY3NbA0NzEzMAQAH8YDgA==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 2, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDCwMLOwMDKztDQHsszNAB+JA4w="
    },
    {
      "FEC_RESULTADO_1": "20200102",
      "UBIGEO": "230101",
      "FEC_RESULTADO_2": "20200102",
      "DISTRITO": "TACNA",
      "EDAD_PACIENTE": "67",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "TACNA",
      "SERVICIO_HOSPITALARIO": "MEDICINA FAMILIAR Y COMUNITARIA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "207.0",
      "IPRESS": "CAP II OSCAR FERNANDEZ DAVILA VELEZ",
      "RESULTADO_2": "113.0",
      "COD_DIAG": "E11.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL TACNA",
      "SEXO_PACIENTE": "MASCULINO",
      "EDAD_MEDICO": "37",
      "PROVINCIA": "TACNA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwztLS0MDE1sjQ3NgICUwMDAB/LA3c=",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 2, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDCwNDU3Mja1MDUyMTCxMAEAHukDdA=="
    }
  ]
}
```

#### Min-Max de colesterol (`/api/hadoop/min-max-colesterol`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/min-max-colesterol",
  "message": "Job completed successfully.",
  "results": [
    {
      "departamento": "AMAZONAS",
      "description": "Min: 0.0, Max: 944.0"
    },
    {
      "departamento": "ANCASH",
      "description": "Min: 35.0, Max: 774.0"
    },
    {
      "departamento": "APURIMAC",
      "description": "Min: 47.0, Max: 655.0"
    },
    {
      "departamento": "AREQUIPA",
      "description": "Min: 0.9, Max: 914.0"
    }
  ],
  "downloadUrl": null
}
```

#### Departamentos sobre el promedio nacional de glucosa (`/api/hadoop/glucosa-sobre-promedio`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/glucosa-sobre-promedio",
  "message": "Job completed successfully.",
  "results": [
    {
      "departamento": "AMAZONAS",
      "description": "Promedio: 169.35 (Superior al nacional de 150.85)"
    },
    {
      "departamento": "APURIMAC",
      "description": "Promedio: 160.77 (Superior al nacional de 150.85)"
    },
    {
      "departamento": "AYACUCHO",
      "description": "Promedio: 152.14 (Superior al nacional de 150.85)"
    },
    {
      "departamento": "CAJAMARCA",
      "description": "Promedio: 157.86 (Superior al nacional de 150.85)"
    }
  ]
}
```

#### Clasificación de riesgo cardiovascular  (`/api/hadoop/clasificacion-riesgo`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/clasificacion-riesgo",
  "message": "Job completed successfully.",
  "results": [
    {
      "avgAge": 62.334521531197275,
      "category": "RIESGO_ALTO"
    },
    {
      "avgAge": 58.30970384373031,
      "category": "RIESGO_BAJO"
    },
    {
      "avgAge": 60.26645531084004,
      "category": "RIESGO_CRITICO"
    },
    {
      "avgAge": 63.30388488018691,
      "category": "RIESGO_MODERADO"
    }
  ],
  "downloadUrl": null
}
```

#### Conteo de predicción simple de reingreso  (`/api/hadoop/prediccion-reingreso`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/prediccion-reingreso",
  "message": "Job completed successfully.",
  "results": [
    {
      "count": 235786,
      "key": "ALTA_PROBABILIDAD_REINGRESO"
    },
    {
      "count": 273927,
      "key": "BAJA_PROBABILIDAD_REINGRESO"
    }
  ],
  "downloadUrl": null
}
```

#### Normalización Min-Max de colesterol por provincia  (`/api/hadoop/normalizacion-minmax-colesterol`)
Request body:
```json
{ }
```
Response:
```json
{
  "success": true,
  "runId": "run123",
  "outputPath": "output/run123/normalizacion-minmax-colesterol",
  "message": "Job completed successfully.",
  "results": [
    {
      "FEC_RESULTADO_1": "FEC_RESULTADO_1",
      "UBIGEO": "UBIGEO",
      "FEC_RESULTADO_2": "FEC_RESULTADO_2",
      "DISTRITO": "DISTRITO",
      "EDAD_PACIENTE": "EDAD_PACIENTE",
      "FECHA_CORTE": "FECHA_CORTE",
      "UNIDADES_1": "UNIDADES_1",
      "AREA_HOSPITALARIA": "AREA_HOSPITALARIA",
      "DEPARTAMENTO": "DEPARTAMENTO",
      "SERVICIO_HOSPITALARIO": "SERVICIO_HOSPITALARIO",
      "UNIDADES_2": "UNIDADES_2",
      "PROCEDIMIENTO_2": "PROCEDIMIENTO_2",
      "PROCEDIMIENTO_1": "PROCEDIMIENTO_1",
      "RESULTADO_1": "RESULTADO_1",
      "IPRESS": "IPRESS",
      "COLESTEROL_NORMALIZADO": "COLESTEROL_NORMALIZADO",
      "RESULTADO_2": "RESULTADO_2",
      "COD_DIAG": "COD_DIAG",
      "FECHA_MUESTRA": "FECHA_MUESTRA",
      "RED": "RED",
      "SEXO_PACIENTE": "SEXO_PACIENTE",
      "EDAD_MEDICO": "EDAD_MEDICO",
      "PROVINCIA": "PROVINCIA",
      "ACTIVIDAD_HOSPITALARIA": "ACTIVIDAD_HOSPITALARIA",
      "ID_MEDICO": "ID_MEDICO",
      "DIAGNOSTICO": "DIAGNOSTICO",
      "ID_PACIENTE": "ID_PACIENTE"
    },
    {
      "FEC_RESULTADO_1": "20200111",
      "UBIGEO": "250107",
      "FEC_RESULTADO_2": "20200111",
      "DISTRITO": "MANANTAY",
      "EDAD_PACIENTE": "51",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "UCAYALI",
      "SERVICIO_HOSPITALARIO": "MEDICINA GENERAL",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "217.0",
      "IPRESS": "CAP I MANANTAY",
      "COLESTEROL_NORMALIZADO": "0.3800",
      "RESULTADO_2": "258.0",
      "COD_DIAG": "E11.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL UCAYALI",
      "SEXO_PACIENTE": "MASCULINO",
      "EDAD_MEDICO": "26",
      "PROVINCIA": "CORONEL PORTILLO",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzNjA2MzE2NLY0NzO2NDWyMAQAHxMDeg==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 2, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDAwtDC0NDMxNzAyMzWxMAQAHqoDcA=="
    },
    {
      "FEC_RESULTADO_1": "20200111",
      "UBIGEO": "250105",
      "FEC_RESULTADO_2": "20200111",
      "DISTRITO": "YARINACOCHA",
      "EDAD_PACIENTE": "48",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "UCAYALI",
      "SERVICIO_HOSPITALARIO": "MEDICINA GENERAL",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "192.0",
      "IPRESS": "P.M. ALAMEDA",
      "COLESTEROL_NORMALIZADO": "0.2800",
      "RESULTADO_2": "171.0",
      "COD_DIAG": "E13.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL UCAYALI",
      "SEXO_PACIENTE": "FEMENINO",
      "EDAD_MEDICO": "26",
      "PROVINCIA": "CORONEL PORTILLO",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwzsjS1NDI2MjE2NTaxNLEwsAQAH4YDgg==",
      "DIAGNOSTICO": "DIABETES MELLITUS ESPECIFICADA, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDAwMjAxNTUxsbQ0NTAzMgUAHnQDbg=="
    },
    {
      "FEC_RESULTADO_1": "20200102",
      "UBIGEO": "230103",
      "FEC_RESULTADO_2": "20200102",
      "DISTRITO": "CALANA",
      "EDAD_PACIENTE": "67",
      "FECHA_CORTE": "20240531",
      "UNIDADES_1": "mg/dL",
      "AREA_HOSPITALARIA": "CONSULTA EXTERNA",
      "DEPARTAMENTO": "TACNA",
      "SERVICIO_HOSPITALARIO": "ENDOCRINOLOGIA",
      "UNIDADES_2": "mg/dL",
      "PROCEDIMIENTO_2": "DOSAJE DE GLUCOSA EN SANGRE, CUANTITATIVO (EXCEPTO CINTA REACTIVA)",
      "PROCEDIMIENTO_1": "DOSAJE DE COLESTEROL TOTAL EN SANGRE COMPLETA O SUERO",
      "RESULTADO_1": "203.0",
      "IPRESS": "H.III DANIEL ALCIDES CARRION",
      "COLESTEROL_NORMALIZADO": "0.2119",
      "RESULTADO_2": "78.0",
      "COD_DIAG": "E11.9",
      "FECHA_MUESTRA": "20200102",
      "RED": "RED ASISTENCIAL TACNA",
      "SEXO_PACIENTE": "MASCULINO",
      "EDAD_MEDICO": "38",
      "PROVINCIA": "TACNA",
      "ACTIVIDAD_HOSPITALARIA": "ATENCION  MEDICA AMBULATORIA",
      "ID_MEDICO": "eJwztLQwNjM1NTY3NbA0NzEzMAQAH8YDgA==",
      "DIAGNOSTICO": "DIABETES MELLITUS TIPO 2, SIN MENCION DE COMPLICACION",
      "ID_PACIENTE": "eJwzNDCwMLOwMDKztDQHsszNAB+JA4w="
    }
  ]
}
```
