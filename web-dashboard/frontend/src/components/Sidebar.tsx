'use client'

import React, { useState } from 'react';
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
} from '@mui/icons-material';

const DRAWER_WIDTH = 280;

interface SidebarProps {
  selectedMenu: string;
  onMenuSelect: (menu: string) => void;
}

export default function Sidebar({ selectedMenu, onMenuSelect }: SidebarProps) {
  const theme = useTheme();

  const menuItems = [
    {
      id: 'dashboard',
      label: 'Dashboard',
      icon: <DashboardIcon />,
      primary: true,
    },
    {
      id: 'projects',
      label: 'Projects',
      icon: <FolderOpen />,
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
            Upgrade
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
                    ? alpha(theme.palette.primary.main, 0.1)
                    : 'transparent',
                  color: selectedMenu === item.id 
                    ? theme.palette.primary.main
                    : theme.palette.text.primary,
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.primary.main, 0.05),
                  },
                  ...(item.primary && {
                    backgroundColor: alpha(theme.palette.primary.main, 0.15),
                    color: theme.palette.primary.main,
                    fontWeight: 600,
                  }),
                }}
              >
                <ListItemIcon
                  sx={{
                    color: selectedMenu === item.id || item.primary
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
                    fontWeight: selectedMenu === item.id || item.primary ? 600 : 400,
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

        {/* Version Info */}
        <Box sx={{ px: 3, pb: 2 }}>
          <Typography variant="caption" color="text.secondary">
            Powered by
          </Typography>
          <Typography variant="caption" color="text.primary" fontWeight="bold">
            <br />
            Biotia 2025.07.23
          </Typography>
        </Box>
      </Box>
    </Drawer>
  );
}