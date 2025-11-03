'use client'

import React, { useState, useEffect, useCallback } from 'react';
import {
  Container,
  Typography,
  Box,
  Card,
  CardContent,
} from '@mui/material';
import {
  Schedule,
} from '@mui/icons-material';

import { WeatherMap } from '@/components/WeatherMap';
import { WeatherStatsCard } from '@/components/WeatherStatsCard';
import { weatherApi } from '@/services/weatherApi';
import { WeatherData, Location, WeatherStats } from '@/types/weather';
import { DashboardPageProps } from './types';

const DashboardPage: React.FC<DashboardPageProps> = ({ onError }) => {
  const [weatherData, setWeatherData] = useState<WeatherData[]>([]);
  const [locations, setLocations] = useState<Location[]>([]);
  const [stats, setStats] = useState<WeatherStats>({} as WeatherStats);
  const [selectedCity, setSelectedCity] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  const loadData = useCallback(async () => {
    try {
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
      onError('Failed to load data. Please try again.');
    }
  }, [onError]);

  // Initial data load and setup auto-refresh
  useEffect(() => {
    // Load data immediately
    loadData();
    
    // Set up auto-refresh interval
    const interval = setInterval(() => {
      loadData();
    }, 5 * 60 * 1000); // 5 minutes
    
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Empty dependency array is intentional - we want this to run once on mount

  return (
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
              Interactive Map
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
  );
};

export default DashboardPage;