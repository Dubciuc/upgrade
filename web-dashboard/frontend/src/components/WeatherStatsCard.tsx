'use client'

import React from 'react';
import { 
  Card, 
  CardContent, 
  Typography, 
  Box,
  Chip 
} from '@mui/material';
import {
  Thermostat,
  Opacity,
  Compress,
  Visibility,
  Air,
  WbSunny
} from '@mui/icons-material';
import { WeatherStats } from '@/types/weather';

interface WeatherStatsCardProps {
  stats: WeatherStats;
}

export default function WeatherStatsCard({ stats }: WeatherStatsCardProps) {
  const statItems = [
    {
      label: 'Total Records',
      value: stats.total_records?.toLocaleString() || 'N/A',
      icon: <WbSunny color="primary" />,
      color: 'primary' as const,
    },
    {
      label: 'Avg Temperature',
      value: stats.avg_temperature ? `${stats.avg_temperature.toFixed(1)}°C` : 'N/A',
      icon: <Thermostat color="error" />,
      color: 'error' as const,
    },
    {
      label: 'Max Temperature',
      value: stats.max_temperature ? `${stats.max_temperature.toFixed(1)}°C` : 'N/A',
      icon: <Thermostat color="error" />,
      color: 'error' as const,
    },
    {
      label: 'Min Temperature',
      value: stats.min_temperature ? `${stats.min_temperature.toFixed(1)}°C` : 'N/A',
      icon: <Thermostat color="info" />,
      color: 'info' as const,
    },
    {
      label: 'Avg Humidity',
      value: stats.avg_humidity ? `${stats.avg_humidity.toFixed(1)}%` : 'N/A',
      icon: <Opacity color="primary" />,
      color: 'primary' as const,
    },
    {
      label: 'Avg Pressure',
      value: stats.avg_pressure ? `${stats.avg_pressure.toFixed(1)} hPa` : 'N/A',
      icon: <Compress color="secondary" />,
      color: 'secondary' as const,
    },
  ];

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Weather Statistics
        </Typography>
        
        <Box sx={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: 2 
        }}>
          {statItems.map((item, index) => (
            <Box 
              key={index}
              sx={{ 
                flex: { xs: '1 1 100%', sm: '1 1 calc(50% - 8px)', md: '1 1 calc(33.333% - 11px)' },
                minWidth: 0 
              }}
            >
              <Box 
                display="flex" 
                alignItems="center" 
                gap={1}
                p={1}
                borderRadius={1}
                sx={{ backgroundColor: 'background.default' }}
              >
                {item.icon}
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    {item.label}
                  </Typography>
                  <Chip 
                    label={item.value}
                    size="small"
                    color={item.color}
                    variant="outlined"
                  />
                </Box>
              </Box>
            </Box>
          ))}
        </Box>

        {stats.last_update && (
          <Box mt={2}>
            <Typography variant="caption" color="text.secondary">
              Last updated: {new Date(stats.last_update).toLocaleString()}
            </Typography>
          </Box>
        )}
      </CardContent>
    </Card>
  );
}