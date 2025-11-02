"""
Run analytical SQL queries against ClickHouse gold layer.
Executes all queries in the sql_queries_clickhouse directory and saves results.
"""
import os
import json
import clickhouse_connect
from datetime import datetime
from pathlib import Path


def get_client():
    """Get ClickHouse client connection."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "mypassword")
    )


def run_query(client, query_file, output_dir="results"):
    """
    Execute a SQL query from file and save results.
    
    Args:
        client: ClickHouse client
        query_file: Path to SQL query file
        output_dir: Directory to save results
    """
    query_name = Path(query_file).stem
    print(f"\n{'='*80}")
    print(f"Running: {query_name}")
    print(f"{'='*80}")
    
    # Read query
    with open(query_file, 'r', encoding='utf-8') as f:
        query = f.read()
    
    print(f"\nQuery:\n{query[:200]}...\n")
    
    try:
        # Execute query
        result = client.query(query)
        
        # Get column names and data
        columns = result.column_names
        rows = result.result_rows
        
        print(f"✅ Query executed successfully")
        print(f"   Rows returned: {len(rows)}")
        print(f"   Columns: {', '.join(columns)}")
        
        # Save results as JSON
        os.makedirs(output_dir, exist_ok=True)
        json_file = os.path.join(output_dir, f"{query_name}_results.json")
        
        results = {
            "query_name": query_name,
            "execution_time": datetime.now().isoformat(),
            "row_count": len(rows),
            "columns": columns,
            "data": [dict(zip(columns, row)) for row in rows]
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"   Results saved to: {json_file}")
        
        # Print sample results (first 5 rows)
        if rows:
            print(f"\n   Sample results (first {min(5, len(rows))} rows):")
            print(f"   {' | '.join(columns[:5])}")
            print(f"   {'-' * 80}")
            for row in rows[:5]:
                print(f"   {' | '.join(str(val)[:20] for val in row[:5])}")
        
        return {
            "query_name": query_name,
            "status": "success",
            "row_count": len(rows),
            "output_file": json_file
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Query failed: {error_msg}")
        
        error_file = os.path.join(output_dir, f"{query_name}_error.json")
        with open(error_file, 'w', encoding='utf-8') as f:
            json.dump({
                "query_name": query_name,
                "execution_time": datetime.now().isoformat(),
                "status": "error",
                "error": error_msg
            }, f, indent=2)
        
        return {
            "query_name": query_name,
            "status": "error",
            "error": error_msg,
            "output_file": error_file
        }


def main():
    """Main function to run all analytical queries."""
    print("="*80)
    print("ClickHouse Analytical Queries Runner")
    print("="*80)
    
    # Get script directory
    script_dir = Path(__file__).parent
    queries_dir = script_dir
    output_dir = script_dir / "results"
    
    # Connect to ClickHouse
    print("\nConnecting to ClickHouse...")
    try:
        client = get_client()
        # Test connection
        test_result = client.query("SELECT 1")
        print(f"✅ Connected to ClickHouse")
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return
    
    # Find all SQL files
    sql_files = sorted(queries_dir.glob("*.sql"))
    if not sql_files:
        print(f"\n❌ No SQL files found in {queries_dir}")
        return
    
    print(f"\nFound {len(sql_files)} SQL query files:")
    for f in sql_files:
        print(f"  - {f.name}")
    
    # Run each query
    results_summary = []
    for sql_file in sql_files:
        result = run_query(client, sql_file, str(output_dir))
        results_summary.append(result)
    
    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    
    successful = [r for r in results_summary if r["status"] == "success"]
    failed = [r for r in results_summary if r["status"] == "error"]
    
    print(f"\nTotal queries: {len(results_summary)}")
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    
    if successful:
        print(f"\nSuccessful queries:")
        for r in successful:
            print(f"  - {r['query_name']}: {r['row_count']} rows → {r['output_file']}")
    
    if failed:
        print(f"\nFailed queries:")
        for r in failed:
            print(f"  - {r['query_name']}: {r['error']}")
    
    # Save summary
    summary_file = output_dir / "execution_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump({
            "execution_time": datetime.now().isoformat(),
            "total_queries": len(results_summary),
            "successful": len(successful),
            "failed": len(failed),
            "results": results_summary
        }, f, indent=2)
    
    print(f"\n📄 Full summary saved to: {summary_file}")
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()


