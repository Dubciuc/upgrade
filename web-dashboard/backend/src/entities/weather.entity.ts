import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, ManyToOne, JoinColumn } from 'typeorm';
import { Location } from './location.entity';

@Entity('weather_data')
export class WeatherData {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 100, nullable: true })
  city: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  country: string;

  @Column({ type: 'decimal', precision: 10, scale: 6, nullable: true })
  latitude: number;

  @Column({ type: 'decimal', precision: 10, scale: 6, nullable: true })
  longitude: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  temperature: number;

  @Column({ type: 'integer', nullable: true })
  humidity: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  wind_speed: number;

  @Column({ type: 'integer', nullable: true })
  wind_direction: number;

  @Column({ type: 'text', nullable: true })
  weather_description: string;

  @CreateDateColumn({ type: 'timestamp with time zone' })
  timestamp: Date;

  @Column({ type: 'varchar', length: 50, default: 'open_meteo' })
  data_source: string;

  @Column({ type: 'jsonb', nullable: true })
  raw_data: any;
}

@Entity('weather_measurements')
export class WeatherMeasurement {
  @PrimaryGeneratedColumn()
  weather_id: number;

  @Column({ type: 'integer', nullable: true })
  location_id: number;

  @ManyToOne(() => Location)
  @JoinColumn({ name: 'location_id' })
  location: Location;

  @Column({ 
    type: 'enum', 
    enum: ['open_meteo', 'manual', 'sensor', 'station'],
    default: 'open_meteo'
  })
  source: string;

  @Column({ type: 'timestamp with time zone' })
  measurement_datetime: Date;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  temperature: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  humidity: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  apparent_temperature: number;

  @Column({ type: 'decimal', precision: 6, scale: 2, nullable: true })
  rainfall: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  windspeed: number;

  @Column({ type: 'integer', nullable: true })
  wind_direction: number;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  wind_gusts: number;

  @Column({ type: 'decimal', precision: 7, scale: 2, nullable: true })
  pressure_msl: number;

  @Column({ type: 'decimal', precision: 7, scale: 2, nullable: true })
  surface_pressure: number;

  @Column({ type: 'integer', nullable: true })
  cloud_cover: number;

  @Column({ type: 'decimal', precision: 8, scale: 2, nullable: true })
  visibility: number;

  @Column({ type: 'decimal', precision: 4, scale: 2, nullable: true })
  uv_index: number;

  @Column({ type: 'integer', nullable: true })
  weather_code: number;

  @Column({ type: 'boolean', nullable: true })
  is_day: boolean;

  @Column({ type: 'varchar', length: 50, nullable: true })
  weather_api_source: string;

  @Column({ type: 'decimal', precision: 3, scale: 2, default: 1.00 })
  quality_score: number;

  @Column({ 
    type: 'enum', 
    enum: ['excellent', 'good', 'fair', 'poor'],
    default: 'good'
  })
  data_quality: string;

  @Column({ type: 'varchar', length: 512, nullable: true })
  raw_data_path: string;

  @Column({ type: 'integer', nullable: true })
  api_response_time_ms: number;

  @CreateDateColumn({ type: 'timestamp with time zone' })
  created_at: Date;
}