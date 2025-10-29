'use client'

import React, { useState, useEffect, useCallback } from 'react';
import {
  Container,
  Typography,
  Box,
  Button,
  Alert,
  Snackbar,
  AppBar,
  Toolbar,
  IconButton,
  Card,
  CardContent,
} from '@mui/material';
import {
  Refresh,
  CloudQueue,
  Schedule,
} from '@mui/icons-material';

import Sidebar from '@/components/Sidebar';
import WeatherMap from '@/components/WeatherMap';
import WeatherStatsCard from '@/components/WeatherStatsCard';
import { weatherApi } from '@/services/weatherApi';
import { WeatherData, Location, WeatherStats } from '@/types/weather';

export default function Dashboard() {
  const [weatherData, setWeatherData] = useState<WeatherData[]>([]);
  const [locations, setLocations] = useState<Location[]>([]);
  const [stats, setStats] = useState<WeatherStats>({} as WeatherStats);
  const [loading, setLoading] = useState(true);
  const [selectedCity, setSelectedCity] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [error, setError] = useState<string | null>(null);
  const [openSnackbar, setOpenSnackbar] = useState(false);
  const [selectedMenu, setSelectedMenu] = useState('dashboard');

  const loadData = useCallback(async () => {
    try {
      setError(null);
      setLoading(true);
      
      const [weatherRes, locationsRes, statsRes] = await Promise.all([
        weatherApi.getLatestWeather(),
        weatherApi.getLocations(),
        weatherApi.getWeatherStats(),
      ]);
      
      setWeatherData(weatherRes);
      setLocations(locationsRes);
      setStats(statsRes);
      setLastUpdate(new Date());
      
    } catch (error) {
      console.error('Error loading data:', error);
      setError('Failed to load data. Please try again.');
      setOpenSnackbar(true);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    
    // Auto-refresh every 5 minutes
    const interval = setInterval(loadData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleRefresh = () => {
    loadData();
  };

  const currentWeather = weatherData.length > 0 ? weatherData[0] : null;

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      {/* Left Sidebar */}
      <Sidebar selectedMenu={selectedMenu} onMenuSelect={setSelectedMenu} />
      
      {/* Main Content */}
      <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column' }}>
        <AppBar position="static" elevation={1} sx={{ zIndex: 1200 }}>
        </AppBar>

        <Container maxWidth="xl" sx={{ mt: 3, mb: 3, flexGrow: 1 }}>
          {/* Header Stats */}
          <Box sx={{ mb: 3 }}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
              
            <Box display="flex" alignItems="center" gap={1}>
              <Schedule color="action" />
              <Typography variant="body2" color="text.secondary">
                Last updated: {lastUpdate.toLocaleTimeString()}
              </Typography>
            </Box>
          </Box>
        </Box>

        {/* Weather Map */}
        <Box sx={{ mb: 3 }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Interactive Weather Map
              </Typography>
              <WeatherMap
                weatherData={weatherData}
                locations={locations}
                selectedCity={selectedCity}
                onCitySelect={setSelectedCity}
              />
            </CardContent>
          </Card>
        </Box>

        {/* Cards Container */}
        <Box sx={{ 
          display: 'flex', 
          flexDirection: { xs: 'column', md: 'row' }, 
          gap: 3, 
          mb: 3 
        }}>

          {/* Weather Statistics */}
          <Box sx={{ flex: 1 }}>
            <WeatherStatsCard stats={stats} />
          </Box>
        </Box>

        {/* Selected City Details */}
        {selectedCity && (
          <Box>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Details for {selectedCity}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Click on markers on the map to see detailed weather information
                </Typography>
              </CardContent>
            </Card>
          </Box>
        )}
        </Container>
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
}
