import { Controller, Get, Query } from '@nestjs/common';
import { WeatherService } from './weather.service';

@Controller('api/weather')
export class WeatherController {
  constructor(private readonly weatherService: WeatherService) {}

  @Get()
  async getWeatherData(@Query('limit') limit?: string, @Query('city') city?: string) {
    const limitNum = limit ? parseInt(limit) : 100;
    let data;
    
    if (city) {
      data = await this.weatherService.getWeatherByCity(city);
    } else {
      data = await this.weatherService.getAllWeatherData(limitNum);
    }
    
    return {
      success: true,
      data
    };
  }

  @Get('latest')
  async getLatestWeatherData() {
    const data = await this.weatherService.getLatestWeatherData();
    return {
      success: true,
      data: data[0] || null
    };
  }

  @Get('stats')
  async getWeatherStats() {
    const data = await this.weatherService.getWeatherStats();
    return {
      success: true,
      data
    };
  }

  @Get('cities')
  async getCities() {
    const cities = await this.weatherService.getCities();
    return {
      success: true,
      data: { cities }
    };
  }
}