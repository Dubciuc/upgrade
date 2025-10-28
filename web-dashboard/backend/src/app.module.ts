import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { DatabaseModule } from './database/database.module';
import { WeatherModule } from './weather/weather.module';
import { LocationsModule } from './locations/locations.module';
import { HealthModule } from './health/health.module';
import { GeojsonModule } from './geojson/geojson.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    ServeStaticModule.forRoot({
      rootPath: join(__dirname, '..', '..', 'frontend', 'dist'),
      exclude: ['/api/*'],
    }),
    DatabaseModule,
    WeatherModule,
    LocationsModule,
    HealthModule,
    GeojsonModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
