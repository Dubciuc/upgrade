export interface WeatherData {
  id?: number;
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  temperature: number;
  humidity: number;
  pressure: number;
  wind_speed: number;
  wind_direction: number;
  weather_condition: string;
  visibility: number;
  uv_index: number;
  timestamp: string;
}

export interface Location {
  id: number;
  city: string;
  country: string;
  latitude: number;
  longitude: number;
}

export interface WeatherStats {
  total_records: number;
  avg_temperature: number;
  max_temperature: number;
  min_temperature: number;
  avg_humidity: number;
  avg_pressure: number;
  last_update: string;
}

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  message?: string;
}