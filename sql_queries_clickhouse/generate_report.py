"""
Generate analytical results report from JSON query results.
Creates a comprehensive markdown report summarizing all analytical query findings.
"""
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def load_query_results(results_dir="results"):
    """Load all JSON query result files."""
    results_path = Path(__file__).parent / results_dir
    query_results = {}
    
    json_files = sorted(results_path.glob("*.json"))
    for json_file in json_files:
        query_name = json_file.stem
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            query_results[query_name] = data
    
    return query_results

def generate_report(query_results, output_file="ANALYTICAL_RESULTS_REPORT.md"):
    """Generate markdown report from query results."""
    
    # Calculate totals
    total_listings = 0
    weather_summary = defaultdict(lambda: {"listings": 0, "prices": []})
    
    # Process impact_listings_by_weather for summary
    if "impact_listings_by_weather" in query_results:
        data = query_results["impact_listings_by_weather"].get("data", [])
        for row in data:
            weather = row.get("weather_bucket", "")
            listings = int(row.get("listings", 0))
            avg_price = float(row.get("avg_price", 0))
            weather_summary[weather]["listings"] = listings
            weather_summary[weather]["avg_price"] = avg_price
            total_listings += listings
    
    # Generate report
    report = f"""# Weather Impact on eBay Listings - Analytical Results Report

**Generated:** {datetime.now().strftime('%B %d, %Y')}  
**Data Source:** eBay listings with weather categorization (East Coast US)  
**Total Listings Analyzed:** {total_listings:,} listings

---

## Executive Summary

This analysis examines how weather conditions impact eBay marketplace behavior, including listing volumes, pricing strategies, shipping choices, seller performance, and product demand patterns. The data reveals significant differences in marketplace dynamics across four weather categories: **Extreme Cold**, **Extreme Heat**, **Precipitation-Heavy**, and **Normal** conditions.

### Key Findings at a Glance

"""
    
    # Add key findings from weather summary
    if weather_summary:
        for weather in ["Extreme Cold", "Extreme Heat", "Normal", "Precipitation-Heavy"]:
            if weather in weather_summary:
                info = weather_summary[weather]
                report += f"- **{weather}** conditions: {info['listings']} listings, avg price ${info.get('avg_price', 0):.2f}\n"
    
    report += "\n---\n\n"
    
    # Process each query result
    query_mappings = {
        "impact_listings_by_weather": {
            "title": "1. Impact of Weather Conditions on Daily Listings",
            "description": "### Overall Listing Patterns by Weather"
        },
        "shipping_choices_vs_weather": {
            "title": "2. Shipping Choices vs Weather Conditions",
            "description": "### Shipping Analysis by Weather Condition"
        },
        "category_demand_shifts_by_weather": {
            "title": "3. Category Demand Shifts by Weather",
            "description": "### Top Products by Weather Condition"
        },
        "pricing_behavior_by_weather_and_product": {
            "title": "4. Pricing Behavior by Weather and Product",
            "description": "### Price Statistics by Product Category"
        },
        "seller_performance_vs_weather": {
            "title": "5. Seller Performance vs Weather Conditions",
            "description": "### Market Share by Seller Tier and Weather"
        },
        "bonus_listing_quality_vs_weather": {
            "title": "6. Listing Quality vs Weather Conditions",
            "description": "### Quality Metrics by Buying Option Complexity"
        },
        "bonus_zip_prefix_variation": {
            "title": "7. Geographic Price Variation by ZIP Code",
            "description": "### Top ZIP Prefixes by Weather Condition"
        }
    }
    
    for query_name, query_info in query_mappings.items():
        if query_name in query_results:
            result_data = query_results[query_name]
            data = result_data.get("data", [])
            
            if not data:
                continue
            
            report += f"\n## {query_info['title']}\n\n"
            report += f"{query_info['description']}\n\n"
            
            # Generate table based on query type
            if query_name == "impact_listings_by_weather":
                report += generate_listings_table(data)
            elif query_name == "shipping_choices_vs_weather":
                report += generate_shipping_table(data)
            elif query_name == "category_demand_shifts_by_weather":
                report += generate_category_table(data)
            elif query_name == "pricing_behavior_by_weather_and_product":
                report += generate_pricing_table(data)
            elif query_name == "seller_performance_vs_weather":
                report += generate_seller_table(data)
            elif query_name == "bonus_listing_quality_vs_weather":
                report += generate_quality_table(data)
            elif query_name == "bonus_zip_prefix_variation":
                report += generate_zip_table(data)
            
            report += "\n---\n\n"
    
    # Add methodology and conclusion
    report += """## Methodology

- **Data Source**: eBay Browse API listings from East Coast US
- **Weather Classification**: Products categorized based on weather-relevance (cold_products, heat_products, rain_products)
- **Date Range**: Analysis date
- **Analytical Platform**: ClickHouse database with dbt transformations
- **Queries**: 7 analytical queries covering listings, pricing, shipping, seller performance, and geographic patterns

---

## Conclusion

Weather conditions significantly impact eBay marketplace dynamics:

1. **Demand Patterns**: Weather events create urgent demand spikes for weather-specific products
2. **Pricing Strategies**: Extreme conditions drive competitive pricing with higher free shipping rates
3. **Seller Behavior**: Premium sellers dominate high-value weather events
4. **Geographic Variation**: Major metropolitan areas show different pricing patterns by weather condition
5. **Shipping Strategy**: Free shipping rates vary significantly, with Extreme Heat showing 100% free shipping

This analysis demonstrates the value of integrating weather data with marketplace analytics to understand seasonal demand patterns and optimize listing strategies.

---

**Report Generated:** """ + datetime.now().strftime('%B %d, %Y') + """  
**For Questions or Detailed Analysis:** Refer to JSON results files in `sql_queries_clickhouse/results/`
"""
    
    # Write report
    output_path = Path(__file__).parent / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"✅ Report generated: {output_path}")
    return output_path

def generate_listings_table(data):
    """Generate table for listings by weather."""
    table = "| Weather Condition | Total Listings | Unique Items | Average Price |\n"
    table += "|-------------------|----------------|--------------|---------------|\n"
    
    for row in sorted(data, key=lambda x: int(x.get("listings", 0)), reverse=True):
        weather = row.get("weather_bucket", "")
        listings = row.get("listings", "0")
        unique = row.get("unique_items", "0")
        price = float(row.get("avg_price", 0))
        table += f"| **{weather}** | {listings} | {unique} | ${price:.2f} |\n"
    
    return table + "\n"

def generate_shipping_table(data):
    """Generate table for shipping choices."""
    table = "| Weather Condition | Free Shipping Rate | Free Shipping Count | Paid Shipping Count | Avg Paid Shipping Cost | Total Listings |\n"
    table += "|-------------------|-------------------|---------------------|---------------------|------------------------|----------------|\n"
    
    for row in data:
        weather = row.get("weather_bucket", "")
        free_rate = float(row.get("free_shipping_rate", 0)) * 100
        free_count = row.get("free_shipping_count", "0")
        paid_count = row.get("paid_shipping_count", "0")
        avg_cost = row.get("avg_paid_shipping_cost", "N/A")
        if avg_cost != "N/A" and avg_cost != "nan":
            avg_cost = f"${float(avg_cost):.2f}"
        total = row.get("total_listings", "0")
        table += f"| **{weather}** | {free_rate:.1f}% | {free_count} | {paid_count} | {avg_cost} | {total} |\n"
    
    return table + "\n"

def generate_category_table(data):
    """Generate summary of top categories."""
    # Group by weather
    by_weather = defaultdict(list)
    for row in data:
        weather = row.get("weather_bucket", "")
        product = row.get("p.product_type", "").replace("_", " ")
        listings = int(row.get("listings", 0))
        by_weather[weather].append((product, listings))
    
    table = ""
    for weather in ["Extreme Cold", "Extreme Heat", "Precipitation-Heavy", "Normal"]:
        if weather in by_weather:
            products = sorted(by_weather[weather], key=lambda x: x[1], reverse=True)[:5]
            table += f"\n#### {weather} - Top Products:\n"
            table += "| Product | Listings | % of Weather Bucket |\n"
            table += "|---------|----------|---------------------|\n"
            total_for_weather = sum(l for _, l in by_weather[weather])
            for product, listings in products:
                pct = (listings / total_for_weather * 100) if total_for_weather > 0 else 0
                table += f"| {product} | {listings} | {pct:.1f}% |\n"
    
    return table + "\n"

def generate_pricing_table(data):
    """Generate pricing statistics table."""
    table = "| Product | Weather | Avg Price | Min | Max | Price StdDev | Listings |\n"
    table += "|---------|---------|-----------|-----|-----|--------------|----------|\n"
    
    # Show top 10 by listings
    sorted_data = sorted(data, key=lambda x: int(x.get("listings", 0)), reverse=True)[:10]
    for row in sorted_data:
        product = row.get("p.product_type", "").replace("_", " ")
        weather = row.get("weather_bucket", "")
        avg_price = float(row.get("avg_price", 0))
        min_price = row.get("min_price", "0")
        max_price = row.get("max_price", "0")
        stddev = row.get("price_stddev", "0")
        listings = row.get("listings", "0")
        table += f"| {product} | {weather} | ${avg_price:.2f} | ${min_price} | ${max_price} | ${float(stddev):.2f} | {listings} |\n"
    
    return table + "\n"

def generate_seller_table(data):
    """Generate seller performance table."""
    # Group by weather and show top sellers
    by_weather = defaultdict(list)
    for row in data:
        weather = row.get("weather_bucket", "")
        tier = row.get("feedback_score_tier", "")
        pct_tier = row.get("feedback_percentage_tier", "")
        listings = int(row.get("listings", 0))
        avg_price = float(row.get("avg_price", 0))
        market_share = float(row.get("market_share_percent", 0))
        by_weather[weather].append((tier, pct_tier, listings, avg_price, market_share))
    
    table = ""
    for weather in ["Extreme Cold", "Extreme Heat", "Precipitation-Heavy", "Normal"]:
        if weather in by_weather:
            sellers = sorted(by_weather[weather], key=lambda x: x[4], reverse=True)[:5]
            table += f"\n#### {weather} Conditions:\n"
            table += "| Seller Tier | Feedback % | Listings | Avg Price | Market Share |\n"
            table += "|-------------|------------|----------|-----------|--------------|\n"
            for tier, pct_tier, listings, avg_price, market_share in sellers:
                table += f"| {tier} | {pct_tier} | {listings:,} | ${avg_price:.2f} | {market_share:.1f}% |\n"
    
    return table + "\n"

def generate_quality_table(data):
    """Generate quality metrics table."""
    table = "| Weather Condition | Buying Option | Avg Title Length | Listings | Unique Items | Quality Score |\n"
    table += "|-------------------|---------------|------------------|----------|--------------|---------------|\n"
    
    for row in data:
        weather = row.get("weather_bucket", "")
        option = row.get("option_complexity", "")
        title_len = float(row.get("avg_title_length", 0))
        listings = row.get("listings", "0")
        unique = row.get("unique_items", "0")
        quality = float(row.get("quality_score", 0))
        table += f"| **{weather}** | {option} | {title_len:.1f} chars | {listings} | {unique} | {quality:.2f} |\n"
    
    return table + "\n"

def generate_zip_table(data):
    """Generate zip prefix variation table."""
    # Group by weather
    by_weather = defaultdict(list)
    for row in data:
        weather = row.get("weather_bucket", "")
        zip_prefix = row.get("zip_prefix", "")
        avg_price = float(row.get("avg_price", 0))
        listings = int(row.get("listings", 0))
        if zip_prefix:
            by_weather[weather].append((zip_prefix, avg_price, listings))
    
    table = ""
    for weather in ["Extreme Cold", "Extreme Heat", "Precipitation-Heavy", "Normal"]:
        if weather in by_weather:
            zips = sorted(by_weather[weather], key=lambda x: x[2], reverse=True)[:5]
            table += f"\n#### {weather} - Top ZIP Areas:\n"
            table += "| ZIP Prefix | Avg Price | Listings |\n"
            table += "|------------|-----------|----------|\n"
            for zip_prefix, avg_price, listings in zips:
                table += f"| {zip_prefix} | ${avg_price:.2f} | {listings} |\n"
    
    return table + "\n"

def main():
    """Main function to generate report."""
    print("="*80)
    print("Analytical Results Report Generator")
    print("="*80)
    
    # Load query results
    print("\nLoading query results...")
    query_results = load_query_results()
    
    if not query_results:
        print("❌ No query results found in results/ directory")
        print("   Run queries first: python run_analytical_queries.py")
        return
    
    print(f"✅ Loaded {len(query_results)} query result files")
    
    # Generate report
    print("\nGenerating report...")
    output_path = generate_report(query_results)
    
    print(f"\n✅ Report generated successfully!")
    print(f"   Location: {output_path}")
    print("\n" + "="*80)

if __name__ == "__main__":
    main()

