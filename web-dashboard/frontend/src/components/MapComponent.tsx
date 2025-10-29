'use client'

import React from 'react';
import { MapContainer, TileLayer, Marker, Popup, CircleMarker } from 'react-leaflet';
import L from 'leaflet';
import { Box } from '@mui/material';
import { WeatherData, Location } from '@/types/weather';
import 'leaflet/dist/leaflet.css';

// Fix for default markers in react-leaflet
if (typeof window !== 'undefined') {
    delete (L.Icon.Default.prototype as any)._getIconUrl;
    L.Icon.Default.mergeOptions({
        iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
        iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
    });
}

interface MapComponentProps {
    weatherData: WeatherData[];
    locations: Location[];
    selectedCity: string | null;
    onCitySelect: (city: string | null) => void;
}
export default function MapComponent({ weatherData, locations, selectedCity, onCitySelect }: MapComponentProps) {
    const getMarkerColor = (temperature: number) => {
        if (temperature < 0) return '#0000ff'; // Blue for very cold
        if (temperature < 10) return '#00ffff'; // Cyan for cold
        if (temperature < 20) return '#00ff00'; // Green for mild
        if (temperature < 30) return '#ffff00'; // Yellow for warm
        return '#ff0000'; // Red for hot
    };

    return (
        <Box sx={{ 
          height: '500px', 
          width: '100%', 
          borderRadius: 2, 
          overflow: 'hidden'
        }}>
            <MapContainer
                center={[46.0, 29.0]}
                zoom={6}
                style={{ height: '100%', width: '100%' }}
                zoomControl={true}
            >
                <TileLayer
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                />

                {/* Weather data markers */}
                {weatherData.map((weather, index) => (
                    <CircleMarker
                            key={`weather-${index}`}
                            center={[weather.latitude, weather.longitude]}
                            radius={10}
                            fillColor={getMarkerColor(weather.temperature)}
                            color="#000"
                            weight={2}
                            opacity={0.8}
                            fillOpacity={0.6}
                            eventHandlers={{
                                click: () => onCitySelect(weather.city),
                            }}
                        >
                            <Popup>
                                <div>
                                    <h3>{weather.city}, {weather.country}</h3>
                                    <p><strong>Temperature:</strong> {weather.temperature}°C</p>
                                    <p><strong>Humidity:</strong> {weather.humidity}%</p>
                                    <p><strong>Pressure:</strong> {weather.pressure} hPa</p>
                                    <p><strong>Wind:</strong> {weather.wind_speed} m/s</p>
                                    <p><strong>Condition:</strong> {weather.weather_condition}</p>
                                    <p><strong>Updated:</strong> {new Date(weather.timestamp).toLocaleString()}</p>
                                </div>
                            </Popup>
                        </CircleMarker>
                    ))}

                    {/* Location markers */}
                    {locations.map((location) => (
                        <Marker
                            key={`location-${location.id}`}
                            position={[location.latitude, location.longitude]}
                            eventHandlers={{
                                click: () => onCitySelect(location.city),
                            }}
                        >
                            <Popup>
                                <div>
                                    <h3>{location.city}, {location.country}</h3>
                                    <p>Available location for weather data</p>
                                </div>
                            </Popup>
                        </Marker>
                    ))}
                </MapContainer>
            </Box>
        );
    }