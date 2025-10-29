'use client'

import React, { useState } from 'react';
import dynamic from 'next/dynamic';
import { 
  Box, 
  CircularProgress, 
  Paper, 
  Typography, 
  Card, 
  CardContent, 
  List, 
  ListItem, 
  ListItemText, 
  ListItemIcon,
  Switch,
  FormControlLabel,
  Divider,
  Chip,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  LocationOn,
  Thermostat,
  Opacity,
  Air,
  Visibility,
  WbSunny,
  FilterList,
  Refresh,
  Settings,
  TravelExplore
} from '@mui/icons-material';
import { WeatherData, Location } from '@/types/weather';

// Dynamically import map to avoid SSR issues
const DynamicMapComponent = dynamic(() => import('./MapComponent'), {
  loading: () => (
    <Box 
      display="flex" 
      justifyContent="center" 
      alignItems="center" 
      height="500px"
    >
      <CircularProgress />
    </Box>
  ),
  ssr: false
});

interface WeatherMapProps {
  weatherData: WeatherData[];
  locations: Location[];
  selectedCity: string | null;
  onCitySelect: (city: string | null) => void;
}

export default function WeatherMap({ weatherData, locations, selectedCity, onCitySelect }: WeatherMapProps) {
  const [showWeatherMarkers, setShowWeatherMarkers] = useState(true);
  const [showLocationMarkers, setShowLocationMarkers] = useState(true);
  const [temperatureFilter, setTemperatureFilter] = useState(false);
  const [showControls, setShowControls] = useState(false);

  const totalLocations = locations.length;
  const totalWeatherPoints = weatherData.length;
  const avgTemperature = weatherData.length > 0 
    ? (weatherData.reduce((sum, data) => sum + data.temperature, 0) / weatherData.length).toFixed(1)
    : 'N/A';

  const handleRefresh = () => {
    // Trigger refresh logic here
    console.log('Refreshing map data...');
  };

  return (
    <Box sx={{ position: 'relative', height: '500px' }}>
      {/* Settings Icon - Floating */}
      <Box 
        sx={{ 
          position: 'absolute', 
          bottom: 16, 
          left: 16, 
          zIndex: 500,
          cursor: 'pointer'
        }}
        onMouseEnter={() => setShowControls(true)}
        onMouseLeave={() => setShowControls(false)}
      >
        <Tooltip title="Map Controls">
          <IconButton
            sx={{
              backgroundColor: 'white',
              boxShadow: 2,
              width: 40,
              height: 40,
              '&:hover': {
                backgroundColor: 'grey.100',
              },
            }}
          >
            <Settings />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Main Container */}
      <Box sx={{ display: 'flex', height: '100%', position: 'relative' }}>
        {/* Collapsible Controls Panel */}
        <Box
          sx={{
            position: 'absolute',
            left: 0,
            top: 0,
            height: '100%',
            width: showControls ? 300 : 0,
            transition: 'width 0.3s ease-in-out',
            overflow: 'hidden',
            zIndex: 400,
          }}
          onMouseEnter={() => setShowControls(true)}
          onMouseLeave={() => setShowControls(false)}
        >
          <Paper 
            elevation={3} 
            sx={{ 
              width: 300, 
              height: '100%',
              p: 2, 
              display: 'flex', 
              flexDirection: 'column',
              overflow: 'auto'
            }}
          >
            {/* Header */}
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <TravelExplore color="primary" />
                Map Controls
              </Typography>
              <Box>
                <Tooltip title="Refresh Data">
                  <IconButton size="small" onClick={handleRefresh}>
                    <Refresh />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>

            {/* Statistics */}
            <Card sx={{ mb: 2 }}>
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Typography variant="subtitle2" gutterBottom color="primary">
                  Statistics
                </Typography>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Total Locations:</Typography>
                    <Chip label={totalLocations} size="small" color="info" />
                  </Box>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Weather Points:</Typography>
                    <Chip label={totalWeatherPoints} size="small" color="success" />
                  </Box>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Typography variant="body2">Avg Temperature:</Typography>
                    <Chip label={`${avgTemperature}°C`} size="small" color="warning" />
                  </Box>
                </Box>
              </CardContent>
            </Card>

            {/* Display Controls */}
            <Card sx={{ mb: 2 }}>
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Typography variant="subtitle2" gutterBottom color="primary">
                  Display Controls
                </Typography>
                <List dense sx={{ p: 0 }}>
                  <ListItem sx={{ px: 0 }}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={showWeatherMarkers}
                          onChange={(e) => setShowWeatherMarkers(e.target.checked)}
                          size="small"
                        />
                      }
                      label="Weather Markers"
                    />
                  </ListItem>
                  <ListItem sx={{ px: 0 }}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={showLocationMarkers}
                          onChange={(e) => setShowLocationMarkers(e.target.checked)}
                          size="small"
                        />
                      }
                      label="Location Markers"
                    />
                  </ListItem>
                  <ListItem sx={{ px: 0 }}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={temperatureFilter}
                          onChange={(e) => setTemperatureFilter(e.target.checked)}
                          size="small"
                        />
                      }
                      label="Temperature Filter"
                    />
                  </ListItem>
                </List>
              </CardContent>
            </Card>

            {/* Location List */}
            <Card sx={{ flex: 1 }}>
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Typography variant="subtitle2" gutterBottom color="primary">
                  Locations ({locations.length})
                </Typography>
                <List dense sx={{ p: 0, maxHeight: 200, overflow: 'auto' }}>
                  {locations.slice(0, 8).map((location) => (
                    <ListItem 
                      key={location.id}
                      onClick={() => onCitySelect(location.city)}
                      sx={{ 
                        px: 1, 
                        borderRadius: 1,
                        mb: 0.5,
                        cursor: 'pointer',
                        backgroundColor: selectedCity === location.city ? 'action.selected' : 'transparent',
                        '&:hover': {
                          backgroundColor: 'action.hover'
                        }
                      }}
                    >
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <LocationOn color="primary" fontSize="small" />
                      </ListItemIcon>
                      <ListItemText 
                        primary={location.city}
                        secondary={location.country}
                        primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                        secondaryTypographyProps={{ variant: 'caption' }}
                      />
                    </ListItem>
                  ))}
                  {locations.length > 8 && (
                    <ListItem>
                      <ListItemText 
                        primary={`... and ${locations.length - 8} more`}
                        primaryTypographyProps={{ variant: 'caption', color: 'text.secondary', fontStyle: 'italic' }}
                      />
                    </ListItem>
                  )}
                </List>
              </CardContent>
            </Card>

            {/* Selected City Info */}
            {selectedCity && weatherData.find(w => w.city === selectedCity) && (
              <Card sx={{ mt: 2 }}>
                <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                  <Typography variant="subtitle2" gutterBottom color="primary">
                    Selected: {selectedCity}
                  </Typography>
                  {(() => {
                    const cityWeather = weatherData.find(w => w.city === selectedCity);
                    return cityWeather ? (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Thermostat fontSize="small" color="error" />
                          <Typography variant="body2">{cityWeather.temperature}°C</Typography>
                        </Box>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Opacity fontSize="small" color="primary" />
                          <Typography variant="body2">{cityWeather.humidity}%</Typography>
                        </Box>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Air fontSize="small" color="info" />
                          <Typography variant="body2">{cityWeather.wind_speed} m/s</Typography>
                        </Box>
                      </Box>
                    ) : null;
                  })()}
                </CardContent>
              </Card>
            )}
          </Paper>
        </Box>

        {/* Map Container */}
        <Box 
          sx={{ 
            flex: 1,
            borderRadius: 2, 
            overflow: 'hidden',
            marginLeft: showControls ? '300px' : '0px',
            transition: 'margin-left 0.3s ease-in-out',
          }}
        >
          <DynamicMapComponent 
            weatherData={showWeatherMarkers ? weatherData : []} 
            locations={showLocationMarkers ? locations : []} 
            selectedCity={selectedCity} 
            onCitySelect={onCitySelect} 
          />
        </Box>
      </Box>
    </Box>
  );
}