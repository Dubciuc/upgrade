import { WeatherData, Location } from '@/types/weather';

export interface WeatherMapProps {
  weatherData: WeatherData[];
  locations: Location[];
  selectedCity: string | null;
  onCitySelect: (city: string | null) => void;
}