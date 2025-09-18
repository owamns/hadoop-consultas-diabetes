import { useState } from 'react'
import './App.css'
import EndpointForm from './components/EndpointForm'
import type { Endpoint } from './components/EndpointForm'
import ResultTable from './components/ResultTable'

function App() {
  const API_BASE = 'http://localhost:8080/api/hadoop'

  const endpoints: Endpoint[] = [
    { key: 'edad-promedio', path: 'edad-promedio', description: 'Obtiene el promedio de edad de pacientes por diagnóstico.', params: [] },
    { key: 'pacientes-depto-sexo', path: 'pacientes-depto-sexo', description: 'Cuenta el número de pacientes por departamento y sexo.', params: [] },
    { key: 'procedimientos-area-servicio', path: 'procedimientos-area-servicio', description: 'Cuenta procedimientos agrupados por área hospitalaria y servicio.', params: [] },
    { key: 'estadisticas-colesterol', path: 'estadisticas-colesterol', description: 'Muestra estadísticas descriptivas para los niveles de colesterol.', params: [] },
    { key: 'busqueda-subtexto', path: 'busqueda-subtexto', description: 'Filtra filas que contengan un texto específico.', params: [{ name: 'searchTerm', type: 'text', placeholder: 'Texto a buscar' }] },
    { key: 'busqueda-fechas', path: 'busqueda-fechas', description: 'Filtra registros por rango de fechas de muestra.', params: [{ name: 'startDate', type: 'date', placeholder: 'Fecha inicio' }, { name: 'endDate', type: 'date', placeholder: 'Fecha fin' }] },
    { key: 'min-max-colesterol', path: 'min-max-colesterol', description: 'Obtiene valores mínimo y máximo de colesterol por departamento.', params: [] },
    { key: 'glucosa-sobre-promedio', path: 'glucosa-sobre-promedio', description: 'Listar departamentos con niveles de glucosa sobre el promedio nacional.', params: [] },
    { key: 'clasificacion-riesgo', path: 'clasificacion-riesgo', description: 'Clasificación de riesgo cardiovascular y edad promedio por categoría.', params: [] },
    { key: 'prediccion-reingreso', path: 'prediccion-reingreso', description: 'Conteo de predicciones de reingreso de pacientes.', params: [] },
    { key: 'normalizacion-minmax-colesterol', path: 'normalizacion-minmax-colesterol', description: 'Normalización Min-Max de niveles de colesterol por provincia.', params: [] },
  ]

  const [results, setResults] = useState<Record<string, unknown>[]>([])
  const [page, setPage] = useState(1)
  const [message, setMessage] = useState('')
  const [downloadUrl, setDownloadUrl] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (endpoint: Endpoint, values: Record<string, string>) => {
    setLoading(true)
    setError('')
    setResults([])
    setDownloadUrl('')  // clear previous download link
    setPage(1)
    try {
      const response = await fetch(`${API_BASE}/${endpoint.path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values),
      })
      if (!response.ok) throw new Error(`Error HTTP ${response.status}`)
      const data = await response.json()
      setResults(Array.isArray(data.results) ? data.results : [])
      setMessage(data.message || '')
      // Use runId returned by backend for download URL
      const runId = data.runId as string
      setDownloadUrl(`${API_BASE}/download/${runId}/${endpoint.path}`)
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="app-container">
      <div className="sidebar">
        <h1>Diabetes Analytics</h1>
        <p>Selecciona una consulta y completa los parámetros.</p>
        <EndpointForm endpoints={endpoints} onSubmit={handleSubmit} />
        {loading && <p>Cargando datos...</p>}
        {error && <p style={{ color: 'var(--nord11)' }}>Error: {error}</p>}
        {message && <p>{message}</p>}
        <div className="download-btn">
          <button disabled={!downloadUrl} onClick={() => window.open(downloadUrl, '_blank')}>
            Descargar resultados
          </button>
        </div>
      </div>
      <div className="results">
        {loading ? (
          <div className="spinner" />
        ) : results.length > 0 ? (
          <>
            <div className="pagination">
              <button onClick={() => setPage(p => p-1)} disabled={page <= 1}>Anterior</button>
              <span>{page}/{Math.ceil(results.length/10)}</span>
              <button onClick={() => setPage(p => p+1)} disabled={page >= Math.ceil(results.length/10)}>Siguiente</button>
            </div>
            <ResultTable data={results.slice((page-1)*10, page*10)} />
          </>
        ) : (
          <p>Sin datos para mostrar</p>
        )}
      </div>
    </div>
  )
}

export default App
