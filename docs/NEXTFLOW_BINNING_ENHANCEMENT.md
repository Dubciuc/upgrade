# Nextflow Pipeline Enhancement: MetaBAT2, CONCOCT, and CheckM Integration

## Overview
Successfully added genome binning and quality assessment capabilities to the existing Nextflow pipeline for environmental genomic surveillance. The pipeline now includes two binning algorithms (MetaBAT2 and CONCOCT) and quality assessment with CheckM.

## New Modules Added

### 1. MetaBAT2 Module (`nextflow/modules/metabat2.nf`)
- **Purpose**: Genome binning using abundance and composition information
- **Container**: `metabat/metabat:2.15`
- **Features**:
  - BWA read mapping to assemblies
  - Depth calculation using `jgi_summarize_bam_contig_depths`
  - Configurable minimum contig length (default: 2500bp)
  - Configurable minimum bin size (default: 200kb)
  - Comprehensive logging and error handling

### 2. CONCOCT Module (`nextflow/modules/concoct.nf`)
- **Purpose**: Alternative binning approach using composition and coverage
- **Container**: `quay.io/biocontainers/concoct:1.1.0--py27h88e4a8a_0`
- **Features**:
  - Contig chunking for improved analysis
  - Coverage table generation
  - Configurable chunk size (default: 10kb) and overlap
  - Clustering and bin extraction
  - Robust error handling for empty results

### 3. CheckM Module (`nextflow/modules/checkm.nf`)
- **Purpose**: Quality assessment of genome bins
- **Container**: `quay.io/biocontainers/checkm-genome:1.2.2--pyhdfd78af_1`
- **Features**:
  - Completeness and contamination analysis
  - Quality summaries with bin statistics
  - Support for both MetaBAT2 and CONCOCT bins
  - Automated quality classification (high/medium quality bins)

## Pipeline Workflow Updates

The enhanced pipeline now follows this workflow:
1. **Quality Control** (NanoPlot) - Raw read assessment
2. **Read Filtering** (Filtlong) - Quality-based filtering
3. **Assembly** (Flye) - De novo genome assembly
4. **Binning** (MetaBAT2 & CONCOCT) - Parallel binning approaches
5. **Quality Assessment** (CheckM) - Bin quality evaluation

## Configuration Parameters

### New parameters added to `nextflow.config`:
```groovy
// MetaBAT2 parameters
metabat2_min_contig = 2500      // Minimum contig length for binning
metabat2_min_bin = 200000       // Minimum bin size

// CONCOCT parameters
concoct_min_contig = 1000       // Minimum contig length for binning
concoct_chunk_size = 10000      // Chunk size for cutting contigs
concoct_overlap_size = 0        // Overlap size for chunks

// CheckM parameters
checkm_extension = 'fa'         // File extension for bin files
```

## Output Structure

The pipeline now generates organized results:
```
results/
├── 01_QC/nanoplot/          # Quality control reports
├── 02_filtered/             # Filtered FASTQ files
├── 03_assembly/             # Flye assembly results
├── 04_binning/
│   ├── metabat2/           # MetaBAT2 bins and logs
│   └── concoct/            # CONCOCT bins and logs
└── 05_quality/
    ├── metabat2/           # CheckM results for MetaBAT2 bins
    └── concoct/            # CheckM results for CONCOCT bins
```

## Key Features

1. **Parallel Binning**: Both MetaBAT2 and CONCOCT run independently for comparison
2. **Quality Assessment**: CheckM evaluates bins from both binners
3. **Configurable Parameters**: All binning parameters can be customized
4. **Error Handling**: Robust handling of empty results and failures
5. **Comprehensive Logging**: Detailed logs for debugging and monitoring
6. **Docker Integration**: All tools run in standardized containers

## Usage Examples

```bash
# Basic run with default parameters
nextflow run main.nf -profile docker --input_dir data/ont_reads

# Custom binning parameters
nextflow run main.nf -profile docker \
  --metabat2_min_contig 3000 \
  --concoct_min_contig 1500 \
  --metabat2_min_bin 300000

# Metagenome assembly with optimized binning
nextflow run main.nf -profile docker \
  --flye_meta true \
  --flye_genome_size '50m' \
  --metabat2_min_contig 2000
```

## Quality Metrics

CheckM provides comprehensive quality metrics:
- **High Quality Bins**: >90% complete, <5% contamination
- **Medium Quality Bins**: >50% complete, <10% contamination
- **Completeness**: Percentage of expected genes present
- **Contamination**: Percentage of unexpected duplicate genes
- **Strain Heterogeneity**: Genetic diversity within bins

## Resource Requirements

Updated resource allocations:
- **MetaBAT2**: 30 CPUs, 80GB RAM, 12h timeout
- **CONCOCT**: 30 CPUs, 80GB RAM, 12h timeout  
- **CheckM**: 30 CPUs, 100GB RAM, 8h timeout

## Next Steps

The enhanced pipeline is ready for:
1. Integration with Airflow for orchestration
2. MinIO integration for data storage
3. Streamlit dashboard for result visualization
4. Database storage of binning results and quality metrics

This enhancement significantly expands the pipeline's capabilities for environmental genomic surveillance and metagenome analysis.