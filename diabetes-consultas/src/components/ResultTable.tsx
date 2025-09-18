import React from 'react'

interface ResultTableProps {
  data: Record<string, any>[]
}

const formatHeader = (key: string) => {
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, char => char.toUpperCase())
}

const ResultTable: React.FC<ResultTableProps> = ({ data }) => {
  if (!data || data.length === 0) return null

  const columns = Object.keys(data[0])

  return (
    <table>
      <thead>
        <tr>
          {columns.map(col => (
            <th key={col}>{formatHeader(col)}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, idx) => (
          <tr key={idx}>
            {columns.map(col => (
              <td key={col}>{row[col] != null ? row[col].toString() : ''}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default ResultTable

