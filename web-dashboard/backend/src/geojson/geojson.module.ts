import { Module } from '@nestjs/common';
import { GeojsonController } from './geojson.controller';
import { WeatherModule } from '../weather/weather.module';

@Module({
  imports: [WeatherModule],
  controllers: [GeojsonController],
})
export class GeojsonModule {}