#!/bin/bash

echo "🚀 Setting up Motherson Intelligence Platform..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm

# Create data directories
mkdir -p data/cache data/vector_store data/pdfs

# Copy .env.example to .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo "⚠️  Please edit .env and add your GEMINI_API_KEY"
fi

echo "✅ Setup complete!"
echo "Next steps:"
echo "1. Edit .env and add your GEMINI_API_KEY"
echo "2. Run: python run.py --ingest"
echo "3. Run: streamlit run src/ui/app.py"