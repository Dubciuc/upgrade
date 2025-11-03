'use client'

import React from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';
import { Box } from '@mui/material';
import { WeatherData, Location } from '@/types/weather';
import { MapComponentProps } from './types';
import 'leaflet/dist/leaflet.css';

// Fix for default markers in react-leaflet
if (typeof window !== 'undefined') {
    delete (L.Icon.Default.prototype as unknown as { _getIconUrl: unknown })._getIconUrl;
    L.Icon.Default.mergeOptions({
        iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
        iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
    });
}

const MapComponent: React.FC<MapComponentProps> = ({ weatherData, locations, onCitySelect }) => {
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
                {weatherData.map((weather, index) => {
                    const markerNumber = index + 1;
                    const numberedIcon = L.divIcon({
                        html: `<div style="
                            background-color: ${getMarkerColor(weather.temperature)};
                            color: white;
                            border: 2px solid #000;
                            border-radius: 50%;
                            width: 24px;
                            height: 24px;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            font-weight: bold;
                            font-size: 12px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.3);
                        ">${markerNumber}</div>`,
                        className: 'weather-marker',
                        iconSize: [24, 24],
                        iconAnchor: [12, 12]
                    });

                    return (
                        <Marker
                            key={`weather-${index}`}
                            position={[weather.latitude, weather.longitude]}
                            icon={numberedIcon}
                            eventHandlers={{
                                click: () => onCitySelect(weather.city),
                            }}
                        >
                            <Popup>
                                <div>
                                    <h3>#{markerNumber} - {weather.city}, {weather.country}</h3>
                                    <p><strong>Temperature:</strong> {weather.temperature}°C</p>
                                    <p><strong>Humidity:</strong> {weather.humidity}%</p>
                                    <p><strong>Pressure:</strong> {weather.pressure} hPa</p>
                                    <p><strong>Wind:</strong> {weather.wind_speed} m/s</p>
                                    <p><strong>Condition:</strong> {weather.weather_condition}</p>
                                    <p><strong>Updated:</strong> {new Date(weather.timestamp).toLocaleString()}</p>
                                </div>
                            </Popup>
                        </Marker>
                    );
                })}

                    {/* Location markers */}
                    {locations.map((location, index) => {
                        const locationNumber = weatherData.length + index + 1; // Continue numbering from weather markers
                        const locationIcon = L.divIcon({
                            html: `<div style="
                                background-color: #6366f1;
                                color: white;
                                border: 2px solid #000;
                                border-radius: 50%;
                                width: 24px;
                                height: 24px;
                                display: flex;
                                align-items: center;
                                justify-content: center;
                                font-weight: bold;
                                font-size: 12px;
                                box-shadow: 0 2px 4px rgba(0,0,0,0.3);
                            ">${locationNumber}</div>`,
                            className: 'location-marker',
                            iconSize: [24, 24],
                            iconAnchor: [12, 12]
                        });

                        return (
                            <Marker
                                key={`location-${location.id}`}
                                position={[location.latitude, location.longitude]}
                                icon={locationIcon}
                                eventHandlers={{
                                    click: () => onCitySelect(location.city),
                                }}
                            >
                                <Popup>
                                    <div>
                                        <h3>#{locationNumber} - {location.city}, {location.country}</h3>
                                        <p>Available location for weather data</p>
                                    </div>
                                </Popup>
                            </Marker>
                        );
                    })}
                </MapContainer>
            </Box>
        );
    };

export { MapComponent };