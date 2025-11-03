'use client'

import React, { useState } from 'react';
import {
  Container,
  Typography,
  Box,
  Card,
  CardContent,
  CardActions,
  Button,
  Chip,
  Avatar,
  IconButton,
  Tooltip,
  Divider,
  Menu,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  FormControl,
  InputLabel,
  Select,
  SelectChangeEvent,
} from '@mui/material';
import {
  Collections,
  Add,
  MoreVert,
  Science,
  Public,
  LocalHospital,
  Schedule,
  Folder,
  Assessment,
  Visibility,
  Edit,
  Delete,
  Archive,
} from '@mui/icons-material';
import { Collection } from './types';

const mockCollections: Collection[] = [
  {
    id: '1',
    name: 'Campus Microbiome Study 2025',
    description: 'Comprehensive microbiome analysis of university campus environments including dining halls, libraries, and dormitories.',
    type: 'environmental',
    status: 'active',
    sampleCount: 156,
    createdAt: new Date('2025-01-15'),
    lastUpdated: new Date('2025-10-30'),
    location: 'University Campus',
    tags: ['microbiome', 'environmental', 'campus'],
  },
  {
    id: '2',
    name: 'Pathogen Detection Pipeline',
    description: 'Automated detection and classification of bacterial and viral pathogens in environmental samples.',
    type: 'genomic',
    status: 'processing',
    sampleCount: 89,
    createdAt: new Date('2025-02-20'),
    lastUpdated: new Date('2025-10-29'),
    location: 'Research Lab A',
    tags: ['pathogens', 'detection', 'automation'],
  },
  {
    id: '3',
    name: 'AMR Surveillance Network',
    description: 'Antimicrobial resistance monitoring across multiple campus locations for public health surveillance.',
    type: 'clinical',
    status: 'active',
    sampleCount: 234,
    createdAt: new Date('2025-03-10'),
    lastUpdated: new Date('2025-10-30'),
    location: 'Multiple Sites',
    tags: ['AMR', 'surveillance', 'public-health'],
  },
  {
    id: '4',
    name: 'Archive Collection 2024',
    description: 'Historical collection of samples from the previous academic year for comparative studies.',
    type: 'environmental',
    status: 'archived',
    sampleCount: 445,
    createdAt: new Date('2024-09-01'),
    lastUpdated: new Date('2024-12-15'),
    location: 'Archive Storage',
    tags: ['historical', 'comparative', 'archive'],
  },
];

const getStatusColor = (status: string) => {
  switch (status) {
    case 'active': return 'success';
    case 'processing': return 'warning';
    case 'archived': return 'info';
    default: return 'default';
  }
};

const getTypeIcon = (type: string) => {
  switch (type) {
    case 'genomic': return <Science />;
    case 'environmental': return <Public />;
    case 'clinical': return <LocalHospital />;
    default: return <Collections />;
  }
};

const CollectionsPage: React.FC = () => {
  const [collections, setCollections] = useState<Collection[]>(mockCollections);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedCollection, setSelectedCollection] = useState<Collection | null>(null);
  const [openDialog, setOpenDialog] = useState(false);
  const [filterStatus, setFilterStatus] = useState<string>('all');
  const [filterType, setFilterType] = useState<string>('all');

  const handleMenuClick = (event: React.MouseEvent<HTMLElement>, collection: Collection) => {
    setAnchorEl(event.currentTarget);
    setSelectedCollection(collection);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedCollection(null);
  };

  const handleStatusChange = (event: SelectChangeEvent) => {
    setFilterStatus(event.target.value);
  };

  const handleTypeChange = (event: SelectChangeEvent) => {
    setFilterType(event.target.value);
  };

  const filteredCollections = collections.filter(collection => {
    const statusMatch = filterStatus === 'all' || collection.status === filterStatus;
    const typeMatch = filterType === 'all' || collection.type === filterType;
    return statusMatch && typeMatch;
  });

  const collectionStats = {
    total: collections.length,
    active: collections.filter(c => c.status === 'active').length,
    processing: collections.filter(c => c.status === 'processing').length,
    totalSamples: collections.reduce((sum, c) => sum + c.sampleCount, 0),
  };

  return (
    <Container maxWidth="xl" sx={{ mt: 3, mb: 3 }}>
      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h4" component="h1" fontWeight="bold">
            Collections
          </Typography>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={() => setOpenDialog(true)}
          >
            New Collection
          </Button>
        </Box>
        
        {/* Stats Overview */}
        <Box 
          sx={{ 
            display: 'flex',
            flexWrap: 'wrap',
            gap: 3,
            mb: 3
          }}
        >
          <Box sx={{ flex: '1 1 250px', minWidth: '250px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'primary.main' }}>
                    <Collections />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {collectionStats.total}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Total Collections
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 250px', minWidth: '250px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'success.main' }}>
                    <Assessment />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {collectionStats.active}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Active Collections
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 250px', minWidth: '250px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'warning.main' }}>
                    <Schedule />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {collectionStats.processing}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Processing
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 250px', minWidth: '250px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'info.main' }}>
                    <Folder />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {collectionStats.totalSamples.toLocaleString()}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Total Samples
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
        </Box>

        {/* Filters */}
        <Box display="flex" gap={2} alignItems="center">
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel>Status</InputLabel>
            <Select
              value={filterStatus}
              label="Status"
              onChange={handleStatusChange}
            >
              <MenuItem value="all">All Status</MenuItem>
              <MenuItem value="active">Active</MenuItem>
              <MenuItem value="processing">Processing</MenuItem>
              <MenuItem value="archived">Archived</MenuItem>
            </Select>
          </FormControl>
          
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel>Type</InputLabel>
            <Select
              value={filterType}
              label="Type"
              onChange={handleTypeChange}
            >
              <MenuItem value="all">All Types</MenuItem>
              <MenuItem value="genomic">Genomic</MenuItem>
              <MenuItem value="environmental">Environmental</MenuItem>
              <MenuItem value="clinical">Clinical</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Box>

      {/* Collections Container */}
      <Box 
        sx={{ 
          display: 'flex',
          flexWrap: 'wrap',
          gap: 3,
        }}
      >
        {filteredCollections.map((collection) => (
          <Box 
            key={collection.id}
            sx={{ 
              flex: '1 1 350px', 
              minWidth: '350px',
              maxWidth: '450px',
            }}
          >
            <Card 
              sx={{ 
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                '&:hover': {
                  boxShadow: 4,
                  transform: 'translateY(-2px)',
                  transition: 'all 0.2s ease-in-out',
                },
              }}
            >
              <CardContent sx={{ flex: 1 }}>
                {/* Header */}
                <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={2}>
                  <Box display="flex" alignItems="center" gap={1}>
                    {getTypeIcon(collection.type)}
                    <Typography variant="h6" component="h2" fontWeight="bold">
                      {collection.name}
                    </Typography>
                  </Box>
                  <IconButton 
                    size="small"
                    onClick={(e) => handleMenuClick(e, collection)}
                  >
                    <MoreVert />
                  </IconButton>
                </Box>

                {/* Status and Type */}
                <Box display="flex" gap={1} mb={2}>
                  <Chip 
                    label={collection.status.charAt(0).toUpperCase() + collection.status.slice(1)}
                    color={getStatusColor(collection.status) as any}
                    size="small"
                  />
                  <Chip 
                    label={collection.type}
                    variant="outlined"
                    size="small"
                  />
                </Box>

                {/* Description */}
                <Typography variant="body2" color="text.secondary" mb={3}>
                  {collection.description}
                </Typography>

                {/* Sample Count */}
                <Box mb={2}>
                  <Typography variant="body2" fontWeight="medium" mb={1}>
                    Sample Count: {collection.sampleCount.toLocaleString()}
                  </Typography>
                </Box>

                {/* Tags */}
                <Box mb={2}>
                  <Typography variant="body2" fontWeight="medium" mb={1}>
                    Tags
                  </Typography>
                  <Box display="flex" flexWrap="wrap" gap={0.5}>
                    {collection.tags.map((tag) => (
                      <Chip 
                        key={tag}
                        label={tag}
                        size="small"
                        variant="outlined"
                      />
                    ))}
                  </Box>
                </Box>

                {/* Location & Last Updated */}
                <Divider sx={{ my: 2 }} />
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2" color="text.secondary">
                    {collection.location}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {collection.lastUpdated.toLocaleDateString()}
                  </Typography>
                </Box>
              </CardContent>

              <CardActions sx={{ pt: 0 }}>
                <Button 
                  size="small" 
                  startIcon={<Visibility />}
                  onClick={() => console.log('View collection:', collection.name)}
                >
                  View Details
                </Button>
                <Button 
                  size="small" 
                  startIcon={<Assessment />}
                  onClick={() => console.log('Analyze collection:', collection.name)}
                >
                  Analyze
                </Button>
              </CardActions>
            </Card>
          </Box>
        ))}
      </Box>

      {/* Context Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
      >
        <MenuItem onClick={handleMenuClose}>
          <Edit fontSize="small" sx={{ mr: 1 }} />
          Edit Collection
        </MenuItem>
        <MenuItem onClick={handleMenuClose}>
          <Archive fontSize="small" sx={{ mr: 1 }} />
          Archive Collection
        </MenuItem>
        <MenuItem onClick={handleMenuClose} sx={{ color: 'error.main' }}>
          <Delete fontSize="small" sx={{ mr: 1 }} />
          Delete Collection
        </MenuItem>
      </Menu>

      {/* New Collection Dialog */}
      <Dialog open={openDialog} onClose={() => setOpenDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Create New Collection</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1 }}>
            <TextField
              label="Collection Name"
              fullWidth
              variant="outlined"
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              rows={3}
              variant="outlined"
            />
            <FormControl fullWidth>
              <InputLabel>Collection Type</InputLabel>
              <Select label="Collection Type">
                <MenuItem value="genomic">Genomic</MenuItem>
                <MenuItem value="environmental">Environmental</MenuItem>
                <MenuItem value="clinical">Clinical</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Location"
              fullWidth
              variant="outlined"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button variant="contained" onClick={() => setOpenDialog(false)}>
            Create Collection
          </Button>
        </DialogActions>
      </Dialog>
    </Container>
  );
};

export default CollectionsPage;