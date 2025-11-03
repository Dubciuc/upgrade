'use client'

import React, { useState } from 'react';
import {
  Box,
  Alert,
  Snackbar,
  AppBar,
} from '@mui/material';


import { Sidebar } from '@/components/Sidebar';
import { DashboardPage } from '@/pages/DashboardPage';
import CollectionsPage from '@/pages/CollectionsPage';
import RunsPage from '@/pages/RunsPage';

const App: React.FC = () => {
  const [selectedMenu, setSelectedMenu] = useState('dashboard');
  const [error, setError] = useState<string | null>(null);
  const [openSnackbar, setOpenSnackbar] = useState(false);

  const handleError = (errorMessage: string) => {
    setError(errorMessage);
    setOpenSnackbar(true);
  };

  const renderContent = () => {
    switch (selectedMenu) {
      case 'dashboard':
        return <DashboardPage onError={handleError} />;
      case 'collections':
        return <CollectionsPage />;
      case 'runs':
        return <RunsPage />;
      case 'public-apps':
        return (
          <div style={{ padding: '24px' }}>
            <h2>Public Applications</h2>
            <p>Coming soon...</p>
          </div>
        );
      case 'mapping':
        return (
          <div style={{ padding: '24px' }}>
            <h2>Mapping & Analysis</h2>
            <p>Coming soon...</p>
          </div>
        );
      case 'settings':
        return (
          <div style={{ padding: '24px' }}>
            <h2>Settings</h2>
            <p>Coming soon...</p>
          </div>
        );
      default:
        return <DashboardPage onError={handleError} />;
    }
  };

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      {/* Left Sidebar */}
      <Sidebar selectedMenu={selectedMenu} onMenuSelect={setSelectedMenu} />
      
      {/* Main Content */}
      <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column' }}>
        <AppBar position="static" elevation={1} sx={{ zIndex: 1200 }}>
        </AppBar>

        {/* Dynamic Content Based on Selected Menu */}
        {renderContent()}
      </Box>

      {/* Error Snackbar */}
      <Snackbar
        open={openSnackbar}
        autoHideDuration={6000}
        onClose={() => setOpenSnackbar(false)}
      >
        <Alert 
          onClose={() => setOpenSnackbar(false)} 
          severity="error" 
          sx={{ width: '100%' }}
        >
          {error}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default App;
