import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Location } from '../entities/location.entity';

@Injectable()
export class LocationsService {
  constructor(
    @InjectRepository(Location)
    private locationRepository: Repository<Location>,
  ) {}

  async getAllLocations() {
    return this.locationRepository.find({
      where: { is_active: true },
      order: { location_name: 'ASC' },
    });
  }

  async getLocationById(id: number) {
    return this.locationRepository.findOne({
      where: { location_id: id, is_active: true },
    });
  }

  async getLocationsByCountry(country: string) {
    return this.locationRepository.find({
      where: { country, is_active: true },
      order: { location_name: 'ASC' },
    });
  }

  async getLocationsByCity(city: string) {
    return this.locationRepository.find({
      where: { city, is_active: true },
      order: { location_name: 'ASC' },
    });
  }
}