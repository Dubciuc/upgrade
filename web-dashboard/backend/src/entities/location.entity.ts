import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('locations')
export class Location {
  @PrimaryGeneratedColumn()
  location_id: number;

  @Column({ type: 'varchar', length: 255 })
  location_name: string;

  @Column({ type: 'varchar', length: 100 })
  country: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  region: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  city: string;

  @Column({ type: 'decimal', precision: 10, scale: 8, nullable: true })
  latitude: number;

  @Column({ type: 'decimal', precision: 11, scale: 8, nullable: true })
  longitude: number;

  @Column({ type: 'integer', nullable: true })
  elevation: number;

  @Column({ type: 'varchar', length: 50, default: 'UTC' })
  timezone: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  campus_area: string;

  @Column({ type: 'varchar', length: 150, nullable: true })
  building_name: string;

  @Column({ type: 'integer', nullable: true })
  floor_level: number;

  @Column({ type: 'varchar', length: 50, nullable: true })
  room_number: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  traffic_density: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  surface_material: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  cleaning_frequency: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  access_type: string;

  @Column({ type: 'varchar', length: 20, default: 'indoor' })
  indoor_outdoor: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  ventilation_type: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  lighting_type: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  occupancy_pattern: string;

  @Column({ type: 'boolean', default: true })
  is_active: boolean;

  @Column({ type: 'jsonb', default: '{}' })
  metadata: any;

  @CreateDateColumn({ type: 'timestamp with time zone' })
  created_at: Date;

  @UpdateDateColumn({ type: 'timestamp with time zone' })
  updated_at: Date;
}