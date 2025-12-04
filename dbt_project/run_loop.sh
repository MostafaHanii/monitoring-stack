#!/bin/bash

echo "🚀 Starting dbt runner loop..."

while true; do
    echo "🔄 Running dbt models..."
    dbt run --profiles-dir .
    
    if [ $? -eq 0 ]; then
        echo "✅ dbt run successful"
    else
        echo "❌ dbt run failed"
    fi
    
    echo "⏳ Sleeping for 1 hour..."
    sleep 3600
done
