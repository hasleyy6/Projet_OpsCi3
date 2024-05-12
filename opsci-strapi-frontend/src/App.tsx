import { useState, useEffect } from 'react'

import { URL, TOKEN } from './conf'
import './App.css'

const formatDate = (date: string) => new Date(date).toLocaleDateString('fr')

type Product = {
  name: string
  description?: string
  stock_available: number
  barcode?: string
  updatedAt: string
  createdAt: string
}

const ProductCard = ({ value }: { value: Product }) => (
  <div className="product-card">
    <div className="product-card-name">{value.name}</div>
    <div className="product-card-desc">{value.description}</div>
    <div className="product-card-stock">
      Stock disponible: <b>{value.stock_available}</b>
    </div>
    <div className="product-card-date">
      <div className="product-card-date">
        Créé le {formatDate(value.createdAt)}
      </div>
      <div className="product-card-date">
        Modifié le {formatDate(value.updatedAt)}
      </div>
    </div>
  </div>
)

const App = () => {
  const [values, setValues] = useState<{ attributes: Product; id: number }[]>(
    []
  )
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()

  useEffect(() => {
    const interval = setInterval(() => {
      fetch(URL + '/api/products', {
        headers: { Authorization: `Bearer ${TOKEN}` },
      })
        .then((r) => r.json())
        .then((v) => {
          setLoading(false)
          setValues(v.data)
          setError(null)
        })
        .catch((err) => {
          console.error(err)
          setLoading(true)
          setError(err.message + ' ' + URL)
        })
    }, 2000)

    return () => {
      clearInterval(interval)
    }
  }, [])
  return (
    <div>
      {loading ? (
        <div className="load-container">
          <div>{error && <div className="error">{error}</div>}</div>
          <div className="loader"></div>
          <div></div>
        </div>
      ) : (
        <div className="card-container">
          {values.map((value) => (
            <ProductCard value={value.attributes} key={value.id} />
          ))}
        </div>
      )}
    </div>
  )
}

export default App
