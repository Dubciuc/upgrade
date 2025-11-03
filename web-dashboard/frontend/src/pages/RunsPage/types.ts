export interface Run {
  id: string;
  name: string;
  pipeline: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  startedAt: Date;
  completedAt?: Date;
  duration?: number; // in minutes
  collection: string;
  sampleCount: number;
  progress?: number; // percentage 0-100
  submittedBy: string;
  parameters: Record<string, any>;
  outputFiles?: string[];
  logFiles?: string[];
  errorMessage?: string;
  priority: 'low' | 'normal' | 'high' | 'urgent';
  tags: string[];
}

export interface Pipeline {
  id: string;
  name: string;
  description: string;
  version: string;
  type: 'quality-control' | 'assembly' | 'annotation' | 'analysis' | 'variant-calling';
  estimatedDuration: number; // in minutes
  parameters: PipelineParameter[];
}

export interface PipelineParameter {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'select' | 'file';
  required: boolean;
  defaultValue?: any;
  options?: string[]; // for select type
  description: string;
}

export interface RunStats {
  total: number;
  running: number;
  queued: number;
  completed: number;
  failed: number;
  totalProcessingTime: number; // in minutes
  avgProcessingTime: number; // in minutes
}