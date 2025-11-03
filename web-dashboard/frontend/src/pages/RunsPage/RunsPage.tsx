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
  LinearProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Collapse,
  Alert,
} from '@mui/material';
import {
  PlayArrow,
  Stop,
  Refresh,
  MoreVert,
  Science,
  Assessment,
  Schedule,
  CheckCircle,
  Error,
  Queue,
  Speed,
  CloudDownload,
  Visibility,
  ExpandMore,
  ExpandLess,
  Warning,
  Add,
  FilterList,
} from '@mui/icons-material';
import { Run, Pipeline, RunStats } from './types';

const mockPipelines: Pipeline[] = [
  {
    id: 'qc-basic',
    name: 'Basic Quality Control',
    description: 'Standard QC pipeline with FastQC, MultiQC, and basic filtering',
    version: '1.2.0',
    type: 'quality-control',
    estimatedDuration: 30,
    parameters: [
      { name: 'min_quality', type: 'number', required: true, defaultValue: 20, description: 'Minimum quality score' },
      { name: 'adapter_trimming', type: 'boolean', required: false, defaultValue: true, description: 'Enable adapter trimming' },
    ],
  },
  {
    id: 'assembly-flye',
    name: 'Flye Assembly Pipeline',
    description: 'Long-read assembly using Flye assembler with polishing',
    version: '2.1.3',
    type: 'assembly',
    estimatedDuration: 180,
    parameters: [
      { name: 'genome_size', type: 'string', required: true, defaultValue: '5m', description: 'Estimated genome size' },
      { name: 'read_type', type: 'select', required: true, options: ['nano-raw', 'nano-corr', 'pacbio-raw'], description: 'Read technology type' },
    ],
  },
  {
    id: 'binning-concoct',
    name: 'CONCOCT Binning',
    description: 'Metagenomic binning using CONCOCT algorithm',
    version: '1.4.2',
    type: 'analysis',
    estimatedDuration: 120,
    parameters: [
      { name: 'kmer_length', type: 'number', required: false, defaultValue: 4, description: 'K-mer length for composition' },
      { name: 'coverage_threshold', type: 'number', required: false, defaultValue: 1.0, description: 'Minimum coverage threshold' },
    ],
  },
];

const mockRuns: Run[] = [
  {
    id: '1',
    name: 'Campus Sample QC - Batch 1',
    pipeline: 'Basic Quality Control',
    status: 'running',
    startedAt: new Date('2025-01-30T09:15:00'),
    collection: 'Campus Microbiome Study 2025',
    sampleCount: 24,
    progress: 67,
    submittedBy: 'alice.chen@university.edu',
    parameters: { min_quality: 25, adapter_trimming: true },
    priority: 'high',
    tags: ['campus', 'batch-1', 'qc'],
  },
  {
    id: '2',
    name: 'Pathogen Assembly Run',
    pipeline: 'Flye Assembly Pipeline',
    status: 'completed',
    startedAt: new Date('2025-01-29T14:30:00'),
    completedAt: new Date('2025-01-29T17:45:00'),
    duration: 195,
    collection: 'Pathogen Detection Pipeline',
    sampleCount: 8,
    submittedBy: 'bob.rodriguez@university.edu',
    parameters: { genome_size: '3m', read_type: 'nano-raw' },
    outputFiles: ['assembly.fasta', 'assembly_info.txt', 'assembly_graph.gfa'],
    priority: 'normal',
    tags: ['pathogen', 'assembly', 'nanopore'],
  },
  {
    id: '3',
    name: 'AMR Surveillance Binning',
    pipeline: 'CONCOCT Binning',
    status: 'failed',
    startedAt: new Date('2025-01-29T08:00:00'),
    completedAt: new Date('2025-01-29T09:30:00'),
    duration: 90,
    collection: 'AMR Surveillance Network',
    sampleCount: 12,
    submittedBy: 'carol.kim@university.edu',
    parameters: { kmer_length: 4, coverage_threshold: 1.5 },
    errorMessage: 'Insufficient memory allocation for large dataset processing',
    logFiles: ['binning.log', 'error.log'],
    priority: 'normal',
    tags: ['AMR', 'binning', 'surveillance'],
  },
  {
    id: '4',
    name: 'Environmental QC - Weekly',
    pipeline: 'Basic Quality Control',
    status: 'queued',
    startedAt: new Date('2025-01-30T11:00:00'),
    collection: 'Campus Microbiome Study 2025',
    sampleCount: 36,
    submittedBy: 'david.park@university.edu',
    parameters: { min_quality: 20, adapter_trimming: true },
    priority: 'low',
    tags: ['environmental', 'weekly', 'routine'],
  },
];

const getStatusColor = (status: string) => {
  switch (status) {
    case 'completed': return 'success';
    case 'running': return 'primary';
    case 'failed': return 'error';
    case 'queued': return 'warning';
    case 'cancelled': return 'default';
    default: return 'default';
  }
};

const getStatusIcon = (status: string) => {
  switch (status) {
    case 'completed': return <CheckCircle />;
    case 'running': return <PlayArrow />;
    case 'failed': return <Error />;
    case 'queued': return <Queue />;
    case 'cancelled': return <Stop />;
    default: return <Schedule />;
  }
};

const getPriorityColor = (priority: string) => {
  switch (priority) {
    case 'urgent': return 'error';
    case 'high': return 'warning';
    case 'normal': return 'info';
    case 'low': return 'default';
    default: return 'default';
  }
};

const formatDuration = (minutes: number): string => {
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  if (hours > 0) {
    return `${hours}h ${mins}m`;
  }
  return `${mins}m`;
};

const RunsPage: React.FC = () => {
  const [runs, setRuns] = useState<Run[]>(mockRuns);
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  const [openDialog, setOpenDialog] = useState(false);
  const [filterStatus, setFilterStatus] = useState<string>('all');
  const [filterPipeline, setFilterPipeline] = useState<string>('all');
  const [selectedPipeline, setSelectedPipeline] = useState<string>('');

  const handleExpandClick = (runId: string) => {
    setExpandedRun(expandedRun === runId ? null : runId);
  };

  const handleStatusChange = (event: SelectChangeEvent) => {
    setFilterStatus(event.target.value);
  };

  const handlePipelineChange = (event: SelectChangeEvent) => {
    setFilterPipeline(event.target.value);
  };

  const filteredRuns = runs.filter(run => {
    const statusMatch = filterStatus === 'all' || run.status === filterStatus;
    const pipelineMatch = filterPipeline === 'all' || run.pipeline === filterPipeline;
    return statusMatch && pipelineMatch;
  });

  const runStats: RunStats = {
    total: runs.length,
    running: runs.filter(r => r.status === 'running').length,
    queued: runs.filter(r => r.status === 'queued').length,
    completed: runs.filter(r => r.status === 'completed').length,
    failed: runs.filter(r => r.status === 'failed').length,
    totalProcessingTime: runs
      .filter(r => r.duration)
      .reduce((sum, r) => sum + (r.duration || 0), 0),
    avgProcessingTime: runs.filter(r => r.duration).length > 0 
      ? runs.filter(r => r.duration).reduce((sum, r) => sum + (r.duration || 0), 0) / runs.filter(r => r.duration).length
      : 0,
  };

  return (
    <Container maxWidth="xl" sx={{ mt: 3, mb: 3 }}>
      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h4" component="h1" fontWeight="bold">
            Pipeline Runs
          </Typography>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={() => setOpenDialog(true)}
          >
            New Run
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
          <Box sx={{ flex: '1 1 200px', minWidth: '200px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'primary.main' }}>
                    <Assessment />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {runStats.total}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Total Runs
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 200px', minWidth: '200px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'primary.main' }}>
                    <PlayArrow />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {runStats.running}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Running
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 200px', minWidth: '200px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'warning.main' }}>
                    <Queue />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {runStats.queued}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Queued
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 200px', minWidth: '200px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'success.main' }}>
                    <CheckCircle />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {runStats.completed}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Completed
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 200px', minWidth: '200px' }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ bgcolor: 'info.main' }}>
                    <Speed />
                  </Avatar>
                  <Box>
                    <Typography variant="h4" fontWeight="bold">
                      {Math.round(runStats.avgProcessingTime)}m
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Avg Duration
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Box>
        </Box>

        {/* Filters */}
        <Box display="flex" gap={2} alignItems="center">
          <FilterList />
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel>Status</InputLabel>
            <Select
              value={filterStatus}
              label="Status"
              onChange={handleStatusChange}
            >
              <MenuItem value="all">All Status</MenuItem>
              <MenuItem value="running">Running</MenuItem>
              <MenuItem value="queued">Queued</MenuItem>
              <MenuItem value="completed">Completed</MenuItem>
              <MenuItem value="failed">Failed</MenuItem>
            </Select>
          </FormControl>
          
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <InputLabel>Pipeline</InputLabel>
            <Select
              value={filterPipeline}
              label="Pipeline"
              onChange={handlePipelineChange}
            >
              <MenuItem value="all">All Pipelines</MenuItem>
              {mockPipelines.map((pipeline) => (
                <MenuItem key={pipeline.id} value={pipeline.name}>
                  {pipeline.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>
      </Box>

      {/* Runs Table */}
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell />
              <TableCell>Run Name</TableCell>
              <TableCell>Pipeline</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Progress</TableCell>
              <TableCell>Collection</TableCell>
              <TableCell>Samples</TableCell>
              <TableCell>Priority</TableCell>
              <TableCell>Started</TableCell>
              <TableCell>Duration</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredRuns.map((run) => (
              <React.Fragment key={run.id}>
                <TableRow hover>
                  <TableCell>
                    <IconButton
                      size="small"
                      onClick={() => handleExpandClick(run.id)}
                    >
                      {expandedRun === run.id ? <ExpandLess /> : <ExpandMore />}
                    </IconButton>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontWeight="medium">
                      {run.name}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {run.pipeline}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip 
                      icon={getStatusIcon(run.status)}
                      label={run.status.charAt(0).toUpperCase() + run.status.slice(1)}
                      color={getStatusColor(run.status) as any}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    {run.status === 'running' && run.progress !== undefined ? (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <LinearProgress 
                          variant="determinate" 
                          value={run.progress} 
                          sx={{ width: 80 }}
                        />
                        <Typography variant="body2">
                          {run.progress}%
                        </Typography>
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        {run.status === 'completed' ? '100%' : '-'}
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {run.collection}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {run.sampleCount}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip 
                      label={run.priority}
                      color={getPriorityColor(run.priority) as any}
                      size="small"
                      variant="outlined"
                    />
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {run.startedAt.toLocaleDateString()}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {run.startedAt.toLocaleTimeString()}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {run.duration ? formatDuration(run.duration) : '-'}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Box display="flex" gap={1}>
                      <Tooltip title="View Details">
                        <IconButton size="small">
                          <Visibility />
                        </IconButton>
                      </Tooltip>
                      {run.outputFiles && (
                        <Tooltip title="Download Results">
                          <IconButton size="small">
                            <CloudDownload />
                          </IconButton>
                        </Tooltip>
                      )}
                      <Tooltip title="More Options">
                        <IconButton size="small">
                          <MoreVert />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
                
                {/* Expanded Row */}
                <TableRow>
                  <TableCell colSpan={11} sx={{ py: 0 }}>
                    <Collapse in={expandedRun === run.id} timeout="auto" unmountOnExit>
                      <Box sx={{ margin: 2 }}>
                        {run.status === 'failed' && run.errorMessage && (
                          <Alert severity="error" sx={{ mb: 2 }}>
                            <Typography variant="body2" fontWeight="medium">
                              Error: {run.errorMessage}
                            </Typography>
                          </Alert>
                        )}
                        
                        <Typography variant="h6" gutterBottom>
                          Run Details
                        </Typography>
                        
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                          <Box>
                            <Typography variant="body2" fontWeight="medium">
                              Submitted By
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              {run.submittedBy}
                            </Typography>
                          </Box>
                          
                          <Box>
                            <Typography variant="body2" fontWeight="medium">
                              Parameters
                            </Typography>
                            <Box>
                              {Object.entries(run.parameters).map(([key, value]) => (
                                <Typography key={key} variant="body2" color="text.secondary">
                                  {key}: {String(value)}
                                </Typography>
                              ))}
                            </Box>
                          </Box>
                          
                          <Box>
                            <Typography variant="body2" fontWeight="medium">
                              Tags
                            </Typography>
                            <Box display="flex" flexWrap="wrap" gap={0.5} mt={0.5}>
                              {run.tags.map((tag) => (
                                <Chip 
                                  key={tag}
                                  label={tag}
                                  size="small"
                                  variant="outlined"
                                />
                              ))}
                            </Box>
                          </Box>
                          
                          {(run.outputFiles || run.logFiles) && (
                            <Box>
                              <Typography variant="body2" fontWeight="medium">
                                Files
                              </Typography>
                              {run.outputFiles && (
                                <Box>
                                  <Typography variant="caption" color="text.secondary">
                                    Output Files:
                                  </Typography>
                                  {run.outputFiles.map((file) => (
                                    <Typography key={file} variant="body2" color="primary">
                                      {file}
                                    </Typography>
                                  ))}
                                </Box>
                              )}
                              {run.logFiles && (
                                <Box>
                                  <Typography variant="caption" color="text.secondary">
                                    Log Files:
                                  </Typography>
                                  {run.logFiles.map((file) => (
                                    <Typography key={file} variant="body2" color="primary">
                                      {file}
                                    </Typography>
                                  ))}
                                </Box>
                              )}
                            </Box>
                          )}
                        </Box>
                      </Box>
                    </Collapse>
                  </TableCell>
                </TableRow>
              </React.Fragment>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      {/* New Run Dialog */}
      <Dialog open={openDialog} onClose={() => setOpenDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Submit New Pipeline Run</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1 }}>
            <TextField
              label="Run Name"
              fullWidth
              variant="outlined"
            />
            
            <FormControl fullWidth>
              <InputLabel>Pipeline</InputLabel>
              <Select 
                value={selectedPipeline}
                label="Pipeline"
                onChange={(e) => setSelectedPipeline(e.target.value)}
              >
                {mockPipelines.map((pipeline) => (
                  <MenuItem key={pipeline.id} value={pipeline.id}>
                    <Box>
                      <Typography variant="body1">{pipeline.name}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        {pipeline.description} • Est. {formatDuration(pipeline.estimatedDuration)}
                      </Typography>
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            
            <FormControl fullWidth>
              <InputLabel>Collection</InputLabel>
              <Select label="Collection">
                <MenuItem value="collection1">Campus Microbiome Study 2025</MenuItem>
                <MenuItem value="collection2">Pathogen Detection Pipeline</MenuItem>
                <MenuItem value="collection3">AMR Surveillance Network</MenuItem>
              </Select>
            </FormControl>
            
            <FormControl fullWidth>
              <InputLabel>Priority</InputLabel>
              <Select label="Priority" defaultValue="normal">
                <MenuItem value="low">Low</MenuItem>
                <MenuItem value="normal">Normal</MenuItem>
                <MenuItem value="high">High</MenuItem>
                <MenuItem value="urgent">Urgent</MenuItem>
              </Select>
            </FormControl>
            
            <TextField
              label="Tags (comma-separated)"
              fullWidth
              variant="outlined"
              placeholder="e.g., batch-processing, quality-control"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button variant="contained" onClick={() => setOpenDialog(false)}>
            Submit Run
          </Button>
        </DialogActions>
      </Dialog>
    </Container>
  );
};

export default RunsPage;