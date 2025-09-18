import React, { useState } from 'react';

export interface Endpoint {
  key: string;
  path: string;
  description: string;
  params: { name: string; type: 'text' | 'date'; placeholder: string }[];
}

interface EndpointFormProps {
  endpoints: Endpoint[];
  onSubmit: (endpoint: Endpoint, values: Record<string, string>) => void;
}

const EndpointForm: React.FC<EndpointFormProps> = ({ endpoints, onSubmit }) => {
  const [selectedKey, setSelectedKey] = useState<string>(endpoints[0].key);
  const selected = endpoints.find(e => e.key === selectedKey)!;
  const [values, setValues] = useState<Record<string,string>>({});

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setValues(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(selected, values);
  };

  return (
    <form onSubmit={handleSubmit} style={{ marginBottom: '1rem' }}>
      <label>
        Seleccionar consulta:
        <select name="endpoint" value={selectedKey} onChange={e => { setSelectedKey(e.target.value); setValues({}); }}>
          {endpoints.map(e => (
            <option key={e.key} value={e.key}>
              {e.key}
            </option>
          ))}
        </select>
      </label>
      <p style={{ fontStyle: 'italic' }}>{selected.description}</p>
      {selected.params.map(param => (
        <div key={param.name}>
          <label>
            {param.name}:
            <input
              name={param.name}
              type={param.type}
              placeholder={param.placeholder}
              value={values[param.name] || ''}
              onChange={handleChange}
              required
            />
          </label>
        </div>
      ))}
      <button type="submit">Ejecutar consulta</button>
    </form>
  );
};

export default EndpointForm;
