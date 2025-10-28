import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { WeatherController } from './weather.controller';
import { WeatherService } from './weather.service';
import { WeatherData, WeatherMeasurement } from '../entities/weather.entity';
import { Location } from '../entities/location.entity';

@Module({
  imports: [TypeOrmModule.forFeature([WeatherData, WeatherMeasurement, Location])],
  controllers: [WeatherController],
  providers: [WeatherService],
  exports: [WeatherService],
})
export class WeatherModule {}