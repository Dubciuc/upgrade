'use client'

import React from 'react';
import { 
  Card, 
  CardContent, 
  Typography, 
  Box,
  Chip,
  Divider
} from '@mui/material';
import {
  Thermostat,
  Opacity,
  Compress,
  Air,
  Visibility,
  Navigation,
  WbSunny
} from '@mui/icons-material';
import { WeatherData } from '@/types/weather';

interface CurrentWeatherCardProps {
  weatherData: WeatherData | null;
}

export default function CurrentWeatherCard({ weatherData }: CurrentWeatherCardProps) {
  if (!weatherData) {
    return (
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Current Weather
          </Typography>
          <Typography color="text.secondary">
            No weather data available
          </Typography>
        </CardContent>
      </Card>
    );
  }

  const getTemperatureColor = (temp: number) => {
    if (temp < 0) return 'info';
    if (temp < 10) return 'primary';
    if (temp < 25) return 'success';
    if (temp < 35) return 'warning';
    return 'error';
  };

  return (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">
            Current Weather
          </Typography>
          <Chip 
            label={weatherData.weather_condition}
            color="primary"
            variant="outlined"
          />
        </Box>

        <Box textAlign="center" mb={3}>
          <Typography variant="h3" component="div" color="primary">
            {weatherData.temperature}°C
          </Typography>
          <Typography variant="h6" color="text.secondary">
            {weatherData.city}, {weatherData.country}
          </Typography>
        </Box>

        <Divider sx={{ my: 2 }} />

        <Box sx={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: 2 
        }}>
          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Opacity color="primary" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Humidity
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.humidity}%
                </Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Compress color="secondary" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Pressure
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.pressure} hPa
                </Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Air color="info" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Wind Speed
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.wind_speed} m/s
                </Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Visibility color="warning" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Visibility
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.visibility} km
                </Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Navigation color="error" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Wind Direction
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.wind_direction}°
                </Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ 
            flex: { xs: '1 1 calc(50% - 8px)', sm: '1 1 calc(25% - 12px)' },
            minWidth: 0 
          }}>
            <Box display="flex" alignItems="center" gap={1}>
              <WbSunny color="warning" />
              <Box>
                <Typography variant="caption" color="text.secondary">
                  UV Index
                </Typography>
                <Typography variant="body2" fontWeight="bold">
                  {weatherData.uv_index}
                </Typography>
              </Box>
            </Box>
          </Box>
        </Box>

        <Box mt={2}>
          <Typography variant="caption" color="text.secondary">
            Last updated: {new Date(weatherData.timestamp).toLocaleString()}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );
}