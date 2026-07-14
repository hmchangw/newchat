import { AuthProvider, useAuth } from '@/context/AuthContext'
import AdminLoginPage from '@/pages/AdminLoginPage'
import AppShell from '@/components/AppShell'
import ErrorBoundary from '@/components/shared/ErrorBoundary'

function AppRoutes() {
  const { session } = useAuth()
  return session ? <AppShell /> : <AdminLoginPage />
}

export default function App() {
  return (
    <ErrorBoundary>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </ErrorBoundary>
  )
}
