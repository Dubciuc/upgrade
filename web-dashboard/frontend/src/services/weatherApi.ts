import axios from 'axios';
import { WeatherData, Location, WeatherStats, ApiResponse } from '@/types/weather';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api';

const apiClient = axios.create({
  baseURL: API_BASE,
  timeout: 10000,
});

export const weatherApi = {
  getLatestWeather: async (): Promise<WeatherData[]> => {
    const response = await apiClient.get<ApiResponse<WeatherData>>('/weather/latest');
    if (response.data.success) {
      // Handle single weather object - convert to array for consistency
      const weatherItem = response.data.data;
      return weatherItem ? [weatherItem] : [];
    }
    throw new Error(response.data.message || 'Failed to fetch weather data');
  },

  getLocations: async (): Promise<Location[]> => {
    const response = await apiClient.get<ApiResponse<Location[]>>('/locations');
    if (response.data.success) {
      return response.data.data || [];
    }
    throw new Error(response.data.message || 'Failed to fetch locations');
  },

  getWeatherStats: async (): Promise<WeatherStats> => {
    const response = await apiClient.get<ApiResponse<WeatherStats>>('/weather/stats');
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error(response.data.message || 'Failed to fetch weather stats');
  },
};