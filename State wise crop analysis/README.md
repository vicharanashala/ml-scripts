# Statistics: KCC Dataset - State-wise Crop Analysis

## Overview
This directory contains state-wise crop query analysis from the KCC (Kisan Call Center) dataset across India. It provides insights into agricultural queries by state and crop type, excluding five major states (Punjab, Karnataka, Maharashtra, Tamil Nadu, Uttar Pradesh, West Bengal).

## Files Structure

### State Crop Files (26 states)

Each state is represented as a separate CSV file with the naming convention: `{STATE_NAME}_crops.csv`

**States Included**:
- Andaman & Nicobar Islands (A_AND_N_ISLANDS_crops.csv)
- Arunachal Pradesh
- Assam
- Bihar
- Chhattisgarh
- Dadra & Nagar Haveli (DAMAN_AND_DIU_crops.csv)
- Delhi
- Goa
- Gujarat
- Haryana
- Himachal Pradesh
- Jammu & Kashmir (JAMMU_AND_KASHMIR_crops.csv)
- Jharkhand (JHARKAND_crops.csv)
- Kerala
- Madhya Pradesh (MADHYA_PRADESH_crops.csv)
- Manipur
- Meghalaya
- Mizoram
- Nagaland
- Odisha
- Puducherry (PUDUCHERRY_crops.csv)
- Rajasthan
- Sikkim
- Telangana
- Tripura
- Uttarakhand
- Uttar Pradesh (UTTAR_PRADESH_crops.csv)

**Excluded States** (analyzed separately):
- Punjab (See `kcc dataset part-1/`)
- Karnataka
- Maharashtra
- Tamil Nadu
- West Bengal

### File Format

Each `{STATE}_crops.csv` contains:

| Column | Description |
|--------|-------------|
| Crop | Name of the crop (Rice, Wheat, Sugarcane, Cotton, etc.) |
| Query Count | Number of queries/calls for this crop |
| Percentage | % of total queries for that state |
| QueryDetails | Additional context about the queries (if available) |

## Data Characteristics

### Coverage
- **Total States**: 26 (excluding 5 major states)
- **Regions Covered**: All major agricultural zones in India
- **Data Points**: Crop-wise query distribution for each state

### Insights by Region

#### Northern States
- **Himachal Pradesh**: Horticulture crops (Apple, Apricot)
- **Jammu & Kashmir**: Saffron, Apple, Walnut
- **Uttarakhand**: Horticulture crops, Spices
- **Haryana**: Wheat, Sugarcane, Rice
- **Rajasthan**: Bajra, Gram, Mustard

#### Southern States
- **Andhra Pradesh**: Rice, Sugarcane, Cotton, Groundnut
- **Telangana**: Rice, Sugarcane, Cotton
- **Kerala**: Coconut, Spices, Rubber
- **Karnataka**: (Separate analysis - major state)

#### Eastern States
- **Assam**: Rice, Tea
- **West Bengal**: (Separate analysis - major state)
- **Bihar**: Rice, Wheat, Sugarcane
- **Jharkhand**: Rice, Maize
- **Odisha**: Rice

#### Western States
- **Gujarat**: Cotton, Sugarcane, Groundnut
- **Goa**: Coconut, Spices
- **Dadra & Nagar Haveli**: Mixed crops

#### North-Eastern States
- **Manipur**: Rice
- **Meghalaya**: Rice
- **Mizoram**: Rice, Spices
- **Nagaland**: Rice, Maize
- **Tripura**: Rice, Jute

#### Island States
- **Andaman & Nicobar**: Coconut, Spices

## Usage

### View State Data
```bash
# View specific state crop analysis
head ASSAM_crops.csv
head KERALA_crops.csv
head RAJASTHAN_crops.csv
```

### Analysis Examples
```bash
# Compare crop queries across states
cat */crops.csv | grep -i "rice"

# View top crops by state
sort -t',' -k2 -nr STATE_crops.csv | head -10
```

## Data Processing

### Source
- Original KCC dataset
- Extracted queries for 26 states
- Aggregated by crop type
- Calculated percentages relative to state total

### Processing Steps
1. Filtered out major states (5 states processed separately)
2. Identified all unique crops per state
3. Counted queries by crop
4. Calculated percentages
5. Generated individual CSV files per state

## Key Statistics

### Crops with Highest Queries (All States Combined)
1. **Rice** - Dominant in Eastern, North-Eastern, Southern states
2. **Wheat** - Northern and Central states
3. **Sugarcane** - Across multiple regions
4. **Cotton** - Western and Central states
5. **Groundnut** - Gujarat, Rajasthan
6. **Vegetables** - Universal across states
7. **Spices** - Southern, North-Eastern states
8. **Horticulture crops** - Himalayan states

### Regional Patterns

| Region | Dominant Crops | Query Volume |
|--------|----------------|--------------|
| North | Wheat, Sugarcane, Rice | High |
| South | Rice, Sugarcane, Spices | High |
| East | Rice, Jute | Medium-High |
| West | Cotton, Groundnut, Sugarcane | High |
| North-East | Rice, Tea | Medium |
| Islands | Coconut, Spices | Low |

## File Naming Convention

```
{STATE_NAME}_crops.csv
```

**Examples**:
- `ASSAM_crops.csv`
- `RAJASTHAN_crops.csv`
- `A_AND_N_ISLANDS_crops.csv` (for Andaman & Nicobar)
- `JAMMU_AND_KASHMIR_crops.csv`
- `MADHYA_PRADESH_crops.csv`

**Note**: State names are in UPPERCASE with underscores for multi-word states.

## Related Data

### Main Analysis
- See `kcc dataset part-1/` for Punjab wheat analysis and semantic grouping

### State Selections
This directory focuses on **26 other states** while major agricultural states are analyzed separately:
- Punjab → `kcc dataset part-1/Punjab-Wheat/`
- Karnataka, Maharashtra, Tamil Nadu, Uttar Pradesh, West Bengal → (Separate analysis)

## Recommendations for Further Analysis

1. **Comparative Analysis**:
   - Compare crop queries across states
   - Identify regional agricultural trends
   - Analyze seasonal patterns by state

2. **Crop-Specific Studies**:
   - Deep dive into specific crops (Rice, Wheat, Cotton)
   - Identify state-wise challenges and solutions
   - Compare farming practices by region

3. **Trend Analysis**:
   - Year-over-year query trends
   - Emerging crops by state
   - Seasonal demand patterns

4. **Integration**:
   - Combine with major state analysis for national overview
   - Link with government agricultural policies
   - Connect with weather and climate data

## Data Quality Notes

- Some states may have lower query volumes due to region-specific crops
- Island states (A&N) may have limited data
- Data represents questions asked to KCC, not total farmer population
- Percentages calculated within each state context

## Last Updated
December 2025

## Contact & Support
For questions about specific state data or crop analysis, refer to individual CSV files in this directory.

## Acknowledgments
- Data source: Kisan Call Center (KCC), Ministry of Agriculture
- Analysis period: 2015-2017
- Processing: Agricultural data analytics
