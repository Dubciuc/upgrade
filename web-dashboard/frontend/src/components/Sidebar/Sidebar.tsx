'use client'

import React from 'react';
import {
  Box,
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Typography,
  Divider,
  useTheme,
  alpha,
} from '@mui/material';
import {
  Dashboard as DashboardIcon,
  FolderOpen,
  Public,
  Category,
  Settings,
  Info,
  TravelExplore,
  Collections,
  PlayArrow,
} from '@mui/icons-material';
import { SidebarProps } from './types';

const DRAWER_WIDTH = 280;

export const Sidebar: React.FC<SidebarProps> = ({ selectedMenu, onMenuSelect }) => {
  const theme = useTheme();

  const menuItems = [
    {
      id: 'dashboard',
      label: 'Dashboard',
      icon: <DashboardIcon />,
    },
    {
      id: 'collections',
      label: 'Collections',
      icon: <Collections />,
    },
    {
      id: 'runs',
      label: 'Runs',
      icon: <PlayArrow />,
    },
    {
      id: 'public-apps',
      label: 'Public apps',
      icon: <Public />,
    },
    {
      id: 'catalogs',
      label: 'Catalogs',
      icon: <Category />,
      hasSubmenu: true,
    },
  ];

  const bottomItems = [
    {
      id: 'tutorial',
      label: 'Tutorial',
      icon: <Info />,
    },
    {
      id: 'support',
      label: 'Support',
      icon: <Settings />,
    },
  ];

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: DRAWER_WIDTH,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: DRAWER_WIDTH,
          boxSizing: 'border-box',
          backgroundColor: theme.palette.grey[50],
          borderRight: `1px solid ${theme.palette.divider}`,
        },
      }}
    >
      {/* Logo/Header */}
      <Box sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            sx={{
              width: 32,
              height: 32,
              backgroundColor: theme.palette.primary.main,
              borderRadius: 1,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <TravelExplore sx={{ color: 'white', fontSize: 20 }} />
          </Box>
          <Typography variant="h5" fontWeight="bold" color="text.primary">
            UPGRADE
          </Typography>
        </Box>
      </Box>

      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        {/* Main Menu Items */}
        <List sx={{ px: 2 }}>
          {menuItems.map((item) => (
            <ListItem key={item.id} disablePadding sx={{ mb: 0.5 }}>
              <ListItemButton
                onClick={() => onMenuSelect(item.id)}
                sx={{
                  borderRadius: 2,
                  py: 1.5,
                  backgroundColor: selectedMenu === item.id 
                    ? alpha(theme.palette.primary.main, 0.15)
                    : 'transparent',
                  color: selectedMenu === item.id 
                    ? theme.palette.primary.main
                    : theme.palette.text.primary,
                  '&:hover': {
                    backgroundColor: selectedMenu === item.id
                      ? alpha(theme.palette.primary.main, 0.2)
                      : alpha(theme.palette.primary.main, 0.05),
                  },
                }}
              >
                <ListItemIcon
                  sx={{
                    color: selectedMenu === item.id
                      ? theme.palette.primary.main
                      : theme.palette.text.secondary,
                    minWidth: 40,
                  }}
                >
                  {item.icon}
                </ListItemIcon>
                <ListItemText 
                  primary={item.label}
                  primaryTypographyProps={{
                    fontWeight: selectedMenu === item.id ? 600 : 400,
                  }}
                />
              </ListItemButton>
            </ListItem>
          ))}
        </List>

        {/* Spacer */}
        <Box sx={{ flex: 1 }} />

        {/* Bottom Menu Items */}
        <Divider sx={{ mx: 2, mb: 2 }} />
        <List sx={{ px: 2, pb: 2 }}>
          {bottomItems.map((item) => (
            <ListItem key={item.id} disablePadding sx={{ mb: 0.5 }}>
              <ListItemButton
                onClick={() => onMenuSelect(item.id)}
                sx={{
                  borderRadius: 2,
                  py: 1,
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.primary.main, 0.05),
                  },
                }}
              >
                <ListItemIcon
                  sx={{
                    color: theme.palette.text.secondary,
                    minWidth: 40,
                  }}
                >
                  {item.icon}
                </ListItemIcon>
                <ListItemText 
                  primary={item.label}
                  primaryTypographyProps={{
                    fontSize: '0.875rem',
                  }}
                />
              </ListItemButton>
            </ListItem>
          ))}
        </List>
      </Box>
    </Drawer>
  );
};

