import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { WeatherData, WeatherMeasurement } from '../entities/weather.entity';
import { Location } from '../entities/location.entity';

@Injectable()
export class WeatherService {
  constructor(
    @InjectRepository(WeatherData)
    private weatherDataRepository: Repository<WeatherData>,
    @InjectRepository(WeatherMeasurement)
    private weatherMeasurementRepository: Repository<WeatherMeasurement>,
    @InjectRepository(Location)
    private locationRepository: Repository<Location>,
  ) {}

  async getAllWeatherData(limit: number = 100) {
    return this.weatherDataRepository.find({
      order: { timestamp: 'DESC' },
      take: limit,
    });
  }

  async getLatestWeatherData() {
    return this.weatherDataRepository.find({
      order: { timestamp: 'DESC' },
      take: 1,
    });
  }

  async getWeatherStats() {
    const result = await this.weatherDataRepository
      .createQueryBuilder('weather')
      .select([
        'AVG(weather.temperature) as avg_temperature',
        'MIN(weather.temperature) as min_temperature',
        'MAX(weather.temperature) as max_temperature',
        'AVG(weather.humidity) as avg_humidity',
        'AVG(weather.wind_speed) as avg_wind_speed',
        'COUNT(*) as total_records',
      ])
      .getRawOne();

    return {
      averageTemperature: parseFloat(result.avg_temperature),
      minTemperature: parseFloat(result.min_temperature),
      maxTemperature: parseFloat(result.max_temperature),
      averageHumidity: parseFloat(result.avg_humidity),
      averageWindSpeed: parseFloat(result.avg_wind_speed),
      totalRecords: parseInt(result.total_records),
    };
  }

  async getWeatherByCity(city?: string) {
    const query = this.weatherDataRepository.createQueryBuilder('weather');
    
    if (city) {
      query.where('LOWER(weather.city) = LOWER(:city)', { city });
    }
    
    return query
      .orderBy('weather.timestamp', 'DESC')
      .take(100)
      .getMany();
  }

  async getCities() {
    const result = await this.weatherDataRepository
      .createQueryBuilder('weather')
      .select('DISTINCT weather.city', 'city')
      .where('weather.city IS NOT NULL')
      .getRawMany();

    return result.map(row => row.city).filter(city => city);
  }

  async getGeoJsonWeatherData() {
    const weatherData = await this.weatherDataRepository
      .createQueryBuilder('weather')
      .where('weather.latitude IS NOT NULL AND weather.longitude IS NOT NULL')
      .orderBy('weather.timestamp', 'DESC')
      .take(1000)
      .getMany();

    const features = weatherData.map(data => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [parseFloat(data.longitude.toString()), parseFloat(data.latitude.toString())],
      },
      properties: {
        city: data.city,
        country: data.country,
        temperature: data.temperature,
        humidity: data.humidity,
        wind_speed: data.wind_speed,
        wind_direction: data.wind_direction,
        weather_description: data.weather_description,
        timestamp: data.timestamp,
        data_source: data.data_source,
      },
    }));

    return {
      type: 'FeatureCollection',
      features,
    };
  }
}