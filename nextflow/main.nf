#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

// Print help message
if (params.help) {
    log.info """
    UPGRADE - Environmental Genomic Surveillance Pipeline
    ================================================
    
    Usage:
    nextflow run main.nf [options]
    
    Input options:
    --input_dir    Path to directory containing ONT FASTQ files (default: test_data/ont_data)
    --outdir       Output directory (default: results)
    
    QC options:
    --nanoplot_format                Format for NanoPlot outputs (default: png)
    
    Filtering options:
    --filtlong_min_length           Minimum read length in bp (default: 1000)
    --filtlong_keep_percent         Percentage of best reads to keep (default: 90)
    --filtlong_min_quality          Minimum mean quality score (default: 10)
    
    Assembly options:
    --flye_mode                     Sequencing technology (default: --nano-raw)
                                    Options: --nano-raw, --nano-hq, --nano-corr,
                                            --pacbio-raw, --pacbio-hifi, --pacbio-corr
    --flye_genome_size              Estimated genome size (default: 5m)
                                    Examples: 5m (5 megabases), 2.6g (2.6 gigabases)
    --flye_iterations               Number of polishing iterations (default: 1)
    --flye_meta                     Enable metagenome mode (default: false)
    
    Resource options:
    --threads      Number of threads (default: 30)
    --memory       Memory allocation (default: 8 GB)
    
    Other options:
    --help         Print this help message
    
    Examples:
    nextflow run main.nf -profile docker --input_dir data/ont_reads --filtlong_min_length 1500
    nextflow run main.nf -profile docker --flye_mode '--nano-hq' --flye_genome_size '10m'
    nextflow run main.nf -profile docker --flye_meta true  # For metagenome assembly
    """
    exit 0
}

// Print pipeline info
log.info """
    UPGRADE PIPELINE - Environmental Genomic Surveillance
    =====================================================
    input_dir              : ${params.input_dir}
    outdir                 : ${params.outdir}
    threads                : ${params.threads}
    
    Filtlong parameters:
    min_length             : ${params.filtlong_min_length} bp
    keep_percent           : ${params.filtlong_keep_percent}%
    min_quality            : ${params.filtlong_min_quality}
    
    Flye parameters:
    mode                   : ${params.flye_mode}
    genome_size            : ${params.flye_genome_size}
    iterations             : ${params.flye_iterations}
    meta_mode              : ${params.flye_meta}
"""

// Import modules
include { NANOPLOT } from './modules/nanoplot.nf'
include { FILTLONG } from './modules/filtlong.nf'
include { FLYE } from './modules/flye.nf'

workflow {
    
    // Create input channel
    ont_reads_ch = Channel
        .fromPath("${params.input_dir}/*.fastq.gz")
        .map { file ->
            def sample_id = file.getBaseName().replaceAll(/\.fastq\.gz$/, '')
            println "Found ONT sample: ${sample_id} with file: ${file}"
            return [sample_id, file]
        }
    
    // Check if we have any input files
    ont_reads_ch.ifEmpty { 
        error "No FASTQ files found in ${params.input_dir}. Please check the input directory." 
    }
    
    // Stage 1: Quality Control with NanoPlot
    NANOPLOT(ont_reads_ch)
    
    // Stage 2: Read Filtering with Filtlong
    FILTLONG(ont_reads_ch)
    
    // Stage 3: De novo Assembly with Flye (using filtered reads)
    FLYE(FILTLONG.out.reads, params.flye_mode)
    
    // Print completion message
    // Print completion message
    // Print completion message
    // Print completion message
    workflow.onComplete {
        def success = workflow?.success ?: false
        def exitStatus = workflow?.exitStatus ?: 0
        def failed = workflow?.stats?.failedCount ?: 0
        def outdir = params?.outdir ?: 'results'
        
        // Считаем успешным если нет failed процессов
        if (failed == 0) {
            log.info """
            Pipeline completed successfully!
            
            Results structure:
            ${outdir}/
            ├── 01_QC/nanoplot/          # Quality control reports
            ├── 02_filtered/             # Filtered FASTQ files and logs
            └── 03_assembly/             # Flye assembly results
            
            Assembly outputs:
            - ${outdir}/03_assembly/*.fasta.gz    # Final assembly
            - ${outdir}/03_assembly/*.gfa.gz      # Assembly graph
            - ${outdir}/03_assembly/*.info.txt    # Assembly statistics
            - ${outdir}/03_assembly/*.flye.log    # Assembly log
            
            Next steps:
            - Review QC reports in ${outdir}/01_QC/nanoplot/
            - Check filtering logs in ${outdir}/02_filtered/
            - Examine assembly statistics in ${outdir}/03_assembly/*.info.txt
            - Assembly is ready for downstream analysis
            """
        } else {
            log.error "Pipeline failed. Check the error messages above."
        }
    }
}