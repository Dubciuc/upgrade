export interface Collection {
  id: string;
  name: string;
  description: string;
  type: 'genomic' | 'environmental' | 'clinical';
  status: 'active' | 'archived' | 'processing';
  sampleCount: number;
  createdAt: Date;
  lastUpdated: Date;
  location: string;
  tags: string[];
}

export interface CollectionsPageProps {}