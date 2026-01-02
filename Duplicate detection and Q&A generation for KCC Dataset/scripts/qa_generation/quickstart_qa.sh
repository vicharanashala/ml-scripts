#!/bin/bash
# Quick Start Script for Punjab Q&A Generation

echo "=================================================="
echo "Punjab Q&A Generation - Quick Start"
echo "=================================================="
echo ""

# Check if API key is provided
if [ -z "$1" ]; then
    echo "❌ Error: Anthropic API key required"
    echo ""
    echo "Usage:"
    echo "  ./quickstart_qa.sh YOUR_ANTHROPIC_API_KEY [num_rows]"
    echo ""
    echo "Examples:"
    echo "  ./quickstart_qa.sh sk-ant-api03-xxxxx 100    # Test with 100 rows"
    echo "  ./quickstart_qa.sh sk-ant-api03-xxxxx        # Process all rows"
    echo ""
    exit 1
fi

API_KEY="$1"
NUM_ROWS="${2:-100}"  # Default to 100 rows for testing

echo "📋 Configuration:"
echo "   Rows to process: $NUM_ROWS"
echo "   API Key: ${API_KEY:0:20}..."
echo ""

# Step 1: Check if dependencies are installed
echo "🔍 Checking dependencies..."
if python3 -c "import anthropic, pandas, pytz, nest_asyncio" 2>/dev/null; then
    echo "✅ All dependencies installed"
else
    echo "📦 Installing dependencies..."
    pip3 install --user -r ../../requirements.txt
    
    if [ $? -eq 0 ]; then
        echo "✅ Dependencies installed successfully"
    else
        echo "❌ Failed to install dependencies"
        echo "   Try manually: pip3 install --user anthropic pandas pytz nest_asyncio"
        exit 1
    fi
fi

echo ""

# Step 2: Check if input file exists
if [ ! -f "../../Data/PB_Combined_Cleaned.csv" ]; then
    echo "❌ Error: Input file not found: Data/PB_Combined_Cleaned.csv"
    echo "   Please run clean_and_concat.py first"
    exit 1
fi

echo "✅ Input file found: Data/PB_Combined_Cleaned.csv"
echo ""

# Step 3: Run the generation script
echo "🚀 Starting Q&A generation..."
echo "   This may take a few minutes..."
echo ""

python3 generate_punjab_qa.py \
    --api-key "$API_KEY" \
    --rows "$NUM_ROWS" \
    --input "../../Data/PB_Combined_Cleaned.csv" \
    --output "../../outputs/qa_results/PB_Test_QA.csv"

if [ $? -eq 0 ]; then
    echo ""
    echo "=================================================="
    echo "✅ SUCCESS!"
    echo "=================================================="
    echo "📁 Output file: outputs/qa_results/PB_Test_QA.csv"
    echo ""
    echo "Next steps:"
    echo "  1. Review the output: head -n 5 outputs/qa_results/PB_Test_QA.csv"
    echo "  2. Check quality of translations"
    echo "  3. If satisfied, process full dataset:"
    echo "     cd scripts/qa_generation"
    echo "     python3 generate_punjab_qa.py --api-key $API_KEY"
    echo ""
else
    echo ""
    echo "❌ Processing failed"
    echo "   Check error messages above"
    exit 1
fi
