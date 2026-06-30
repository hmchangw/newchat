import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import { ThemeProvider } from './context/ThemeContext'
import { DebugProvider } from './context/DebugContext'
import { initTelemetry } from './lib/telemetry'
import './styles/tokens.css'
import './styles/index.css'

initTelemetry()

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <ThemeProvider>
      <DebugProvider>
        <App />
      </DebugProvider>
    </ThemeProvider>
  </StrictMode>,
)
