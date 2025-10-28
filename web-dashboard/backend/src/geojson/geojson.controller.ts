import { Controller, Get } from '@nestjs/common';
import { WeatherService } from '../weather/weather.service';

@Controller('api/geojson')
export class GeojsonController {
  constructor(private readonly weatherService: WeatherService) {}

  @Get('weather')
  async getWeatherGeoJson() {
    return this.weatherService.getGeoJsonWeatherData();
  }
}