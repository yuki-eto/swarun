import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import TestRunDetail from './pages/TestRunDetail'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="runs/:id" element={<TestRunDetail />} />
      </Route>
    </Routes>
  )
}

export default App
