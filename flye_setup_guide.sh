#!/bin/bash

# Test script to demonstrate Flye installation and usage
# This script shows different installation methods and usage examples

echo "=== Flye Installation and Usage Guide ==="
echo ""

echo "1. Testing Docker-based Flye installation:"
echo "   docker run --rm staphb/flye:2.9.2 flye --help"
echo ""

echo "2. Alternative installation methods:"
echo ""
echo "   Method A: Conda (recommended for local development)"
echo "   conda install -c bioconda flye"
echo ""
echo "   Method B: pip"
echo "   pip install flye"
echo ""
echo "   Method C: From source"
echo "   git clone https://github.com/fenderglass/Flye.git"
echo "   cd Flye"
echo "   python setup.py install"
echo ""

echo "3. Basic Flye usage examples:"
echo ""
echo "   For Oxford Nanopore raw reads:"
echo "   flye --nano-raw reads.fastq --out-dir flye_output --genome-size 5m"
echo ""
echo "   For Oxford Nanopore high-quality reads (Guppy5+ SUP):"
echo "   flye --nano-hq reads.fastq --out-dir flye_output --genome-size 5m"
echo ""
echo "   For PacBio HiFi reads:"
echo "   flye --pacbio-hifi reads.fastq --out-dir flye_output --genome-size 5m"
echo ""
echo "   For metagenome assembly:"
echo "   flye --nano-raw reads.fastq --out-dir flye_output --meta"
echo ""

echo "4. Flye with your current Nextflow pipeline:"
echo "   Your pipeline now includes Flye! Usage:"
echo "   nextflow run main.nf -profile docker --flye_mode '--nano-raw' --flye_genome_size '5m'"
echo ""

echo "5. Key Flye parameters explanation:"
echo "   --genome-size: Estimated genome size (5m = 5MB, 2.6g = 2.6GB)"
echo "   --threads: Number of parallel threads"
echo "   --iterations: Number of polishing iterations (default: 1)"
echo "   --meta: Enable metagenome mode for uneven coverage"
echo "   --scaffold: Enable scaffolding (experimental)"
echo "   --keep-haplotypes: Don't collapse alternative haplotypes"
echo ""

echo "6. Flye output files:"
echo "   assembly.fasta: Final polished assembly"
echo "   assembly_graph.gfa: Assembly graph in GFA format" 
echo "   assembly_info.txt: Summary statistics for contigs"
echo "   flye.log: Detailed log of the assembly process"
echo ""

echo "7. Testing your Docker installation:"
if docker --version >/dev/null 2>&1; then
    echo "   ✓ Docker is installed"
    if docker images | grep -q staphb/flye; then
        echo "   ✓ Flye Docker image is available"
        echo "   Running quick test..."
        docker run --rm staphb/flye:2.9.2 flye --version
    else
        echo "   ⚠ Flye Docker image not found. Run: docker pull staphb/flye:2.9.2"
    fi
else
    echo "   ✗ Docker not found. Please install Docker first."
fi

echo ""
echo "=== Installation Complete! ==="
echo "You can now use Flye in your genomics pipeline."