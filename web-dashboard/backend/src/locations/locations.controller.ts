import { Controller, Get, Param, Query } from '@nestjs/common';
import { LocationsService } from './locations.service';

@Controller('api/locations')
export class LocationsController {
  constructor(private readonly locationsService: LocationsService) {}

  @Get()
  async getLocations(@Query('country') country?: string, @Query('city') city?: string) {
    let data;
    
    if (country) {
      data = await this.locationsService.getLocationsByCountry(country);
    } else if (city) {
      data = await this.locationsService.getLocationsByCity(city);
    } else {
      data = await this.locationsService.getAllLocations();
    }
    
    return {
      success: true,
      data
    };
  }

  @Get(':id')
  async getLocationById(@Param('id') id: string) {
    const locationId = parseInt(id);
    const data = await this.locationsService.getLocationById(locationId);
    return {
      success: true,
      data
    };
  }
}