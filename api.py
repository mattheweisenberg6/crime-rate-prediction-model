from flask import Flask, jsonify, request
import psycopg2
import psycopg2.extras
from datetime import datetime
import json, os

# Initialize Flask app
app = Flask(__name__)

# Database configuration - update with your credentials
DB_CONFIG = {
    'host': 'localhost',
    'database': 'phoenix_crime_db',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),  # Update this
    'port': '5432'
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def dict_cursor(conn):
    """Get a cursor that returns results as dictionaries"""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

app.json_encoder = DateTimeEncoder

# Routes

# Add these endpoints to your existing Flask API (main.py)

# Add this to your existing routes list in the home() function
@app.route('/')
def home():
    """Welcome endpoint"""
    return jsonify({
        'message': 'Phoenix Crime Data API',
        'endpoints': [
            '/api/health',
            '/api/stats', 
            '/api/crimes',
            '/api/crime-types',
            '/api/crimes/recent',
            '/api/crimes/by-zip',
            '/api/crimes/timeline',
            '/api/update-status',      # NEW
            '/api/trigger-update',     # NEW  
            '/api/data-freshness'      # NEW
        ],
        'data_source': 'City of Phoenix Open Data Portal',
        'update_frequency': 'Daily at 11:30 AM MST'
    })

@app.route('/api/update-status')
def update_status():
    """Get information about the last data update"""
    try:
        import json
        from datetime import datetime
        
        # Read update status from updater
        status_file = 'update_status.json'
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                last_update = json.load(f)
        else:
            last_update = {"message": "No update history found"}
        
        # Get database stats
        conn = get_db_connection()
        if conn:
            cur = dict_cursor(conn)
            
            # Get total count and date range
            cur.execute("SELECT COUNT(*) as total FROM crimes")
            total = cur.fetchone()['total']
            
            cur.execute("SELECT MAX(occurred_date) as latest FROM crimes")
            latest = cur.fetchone()['latest']
            
            conn.close()
            
            database_info = {
                'total_records': total,
                'latest_crime_date': latest,
                'last_query_time': datetime.now().isoformat()
            }
        else:
            database_info = {"error": "Database connection failed"}
        
        return jsonify({
            'phoenix_data_source': {
                'url': 'https://phoenixopendata.com/dataset/crime-data',
                'update_schedule': 'Daily at 11:00 AM MST',
                'data_lag': '7 days (Phoenix provides data through 7 days prior)',
                'date_range': 'November 1, 2015 forward'
            },
            'our_update_schedule': 'Daily at 11:30 AM MST (30 minutes after Phoenix)',
            'last_update': last_update,
            'database_info': database_info,
            'update_method': 'Incremental (only new records processed)'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trigger-update', methods=['POST'])
def trigger_manual_update():
    """Manually trigger a data update (for testing/admin use)"""
    try:
        from phoenix_crime_updater import PhoenixCrimeUpdater
        import threading
        
        def run_update_async():
            updater = PhoenixCrimeUpdater()
            updater.run_update()
        
        # Run update in background thread to avoid timeout
        thread = threading.Thread(target=run_update_async)
        thread.start()
        
        return jsonify({
            'status': 'Update triggered',
            'message': 'Update is running in the background. Check /api/update-status for results.',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data-freshness')
def data_freshness():
    """Check how fresh/current our crime data is"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = dict_cursor(conn)
        
        # Get the most recent crime date
        cur.execute("SELECT MAX(occurred_date) as latest_crime FROM crimes")
        latest_crime = cur.fetchone()['latest_crime']
        
        # Get data distribution by day for last 30 days
        cur.execute("""
            SELECT DATE(occurred_date) as date, COUNT(*) as count
            FROM crimes 
            WHERE occurred_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE(occurred_date)
            ORDER BY date DESC
            LIMIT 30
        """)
        recent_days = cur.fetchall()
        
        conn.close()
        
        # Calculate data freshness
        if latest_crime:
            from datetime import datetime
            latest_date = latest_crime.date() if hasattr(latest_crime, 'date') else latest_crime
            days_old = (datetime.now().date() - latest_date).days
            
            freshness_status = "very_fresh" if days_old <= 7 else "fresh" if days_old <= 14 else "stale"
        else:
            days_old = None
            freshness_status = "unknown"
        
        return jsonify({
            'latest_crime_date': latest_crime,
            'days_since_latest': days_old,
            'freshness_status': freshness_status,
            'expected_lag': '7 days (Phoenix policy)',
            'recent_daily_counts': [dict(day) for day in recent_days]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def health():
    """Health check endpoint"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    else:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500

@app.route('/api/stats')
def get_stats():
    """Get basic crime statistics"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = dict_cursor(conn)
        
        # Total crimes
        cur.execute("SELECT COUNT(*) as total FROM crimes")
        total = cur.fetchone()['total']
        
        # Date range
        cur.execute("SELECT MIN(occurred_date) as earliest, MAX(occurred_date) as latest FROM crimes")
        date_range = cur.fetchone()
        
        # Top crime types
        cur.execute("""
            SELECT crime_type, COUNT(*) as count 
            FROM crimes 
            GROUP BY crime_type 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_crimes = cur.fetchall()
        
        # Crimes by year
        cur.execute("""
            SELECT EXTRACT(YEAR FROM occurred_date) as year, COUNT(*) as count
            FROM crimes 
            GROUP BY EXTRACT(YEAR FROM occurred_date)
            ORDER BY year DESC
        """)
        yearly_stats = cur.fetchall()
        
        return jsonify({
            'total_crimes': total,
            'date_range': {
                'earliest': date_range['earliest'],
                'latest': date_range['latest']
            },
            'top_crime_types': [dict(crime) for crime in top_crimes],
            'crimes_by_year': [dict(year) for year in yearly_stats]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

@app.route('/api/crime-types')
def get_crime_types():
    """Get list of all crime types"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = dict_cursor(conn)
        cur.execute("SELECT DISTINCT crime_type FROM crimes ORDER BY crime_type")
        crime_types = [row['crime_type'] for row in cur.fetchall()]
        
        return jsonify({
            'crime_types': crime_types,
            'count': len(crime_types)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

@app.route('/api/crimes')
def get_crimes():
    """Get crimes with pagination and optional filters"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Parse query parameters
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)  # Max 100 per page
        crime_type = request.args.get('crime_type')
        zip_code = request.args.get('zip_code')
        year = request.args.get('year', type=int)
        
        # Build query
        where_conditions = []
        params = []
        
        if crime_type:
            where_conditions.append("crime_type ILIKE %s")
            params.append(f"%{crime_type}%")
        
        if zip_code:
            where_conditions.append("zip_code = %s")
            params.append(zip_code)
        
        if year:
            where_conditions.append("EXTRACT(YEAR FROM occurred_date) = %s")
            params.append(year)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Count total matching records
        count_query = f"SELECT COUNT(*) as total FROM crimes {where_clause}"
        cur = dict_cursor(conn)
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        # Calculate pagination
        offset = (page - 1) * per_page
        total_pages = (total + per_page - 1) // per_page
        
        # Get data
        data_query = f"""
            SELECT incident_id, crime_type, occurred_date, address, zip_code, premise_type, grid_id
            FROM crimes 
            {where_clause}
            ORDER BY occurred_date DESC 
            LIMIT %s OFFSET %s
        """
        
        cur.execute(data_query, params + [per_page, offset])
        crimes = cur.fetchall()
        
        return jsonify({
            'crimes': [dict(crime) for crime in crimes],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_records': total,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            },
            'filters': {
                'crime_type': crime_type,
                'zip_code': zip_code,
                'year': year
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

@app.route('/api/crimes/recent')
def get_recent_crimes():
    """Get most recent crimes"""
    limit = min(request.args.get('limit', 20, type=int), 100)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = dict_cursor(conn)
        cur.execute("""
            SELECT incident_id, crime_type, occurred_date, address, zip_code, premise_type
            FROM crimes 
            ORDER BY occurred_date DESC 
            LIMIT %s
        """, (limit,))
        
        recent_crimes = cur.fetchall()
        
        return jsonify({
            'recent_crimes': [dict(crime) for crime in recent_crimes],
            'count': len(recent_crimes)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

@app.route('/api/crimes/by-zip')
def crimes_by_zip():
    """Get crime counts by ZIP code"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = dict_cursor(conn)
        cur.execute("""
            SELECT zip_code, COUNT(*) as crime_count
            FROM crimes 
            WHERE zip_code IS NOT NULL
            GROUP BY zip_code 
            ORDER BY crime_count DESC 
            LIMIT 20
        """)
        
        zip_stats = cur.fetchall()
        
        return jsonify({
            'zip_code_stats': [dict(stat) for stat in zip_stats]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

@app.route('/api/crimes/timeline')
def crimes_timeline():
    """Get crimes aggregated by month"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cur = dict_cursor(conn)
        cur.execute("""
            SELECT 
                DATE_TRUNC('month', occurred_date) as month,
                COUNT(*) as crime_count
            FROM crimes 
            GROUP BY DATE_TRUNC('month', occurred_date)
            ORDER BY month DESC
            LIMIT 24
        """)
        
        timeline = cur.fetchall()
        
        return jsonify({
            'timeline': [dict(item) for item in timeline]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        conn.close()

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("🚀 Starting Phoenix Crime Data API")
    print("Available endpoints:")
    print("- http://localhost:5000/")
    print("- http://localhost:5000/api/health")
    print("- http://localhost:5000/api/stats")
    print("- http://localhost:5000/api/crimes")
    print("- http://localhost:5000/api/crime-types")
    
    app.run(debug=True, port=5000)