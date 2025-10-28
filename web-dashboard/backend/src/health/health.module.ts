import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HealthController } from './health.controller';
import { HealthService } from './health.service';
import { Location } from '../entities/location.entity';

@Module({
  imports: [TypeOrmModule.forFeature([Location])],
  controllers: [HealthController],
  providers: [HealthService],
})
export class HealthModule {}