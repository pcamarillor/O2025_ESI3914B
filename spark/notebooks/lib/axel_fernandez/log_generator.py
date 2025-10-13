'''
This module provides a function to generate log messages with timestamps. as the SESSION 14 of Big Data Analytics course.
'''
import csv
import random
import datetime
import os
import time

def generate_logs(min_entries=1, max_entries=50):

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'producer'))
    output_dir = base_dir
    
    os.makedirs(output_dir, exist_ok=True)
    
    log_levels = ['INFO', 'WARN', 'ERROR', 'CRITICAL']
    
    servers = [f'server-node-{i}' for i in range(1, 6)]
    
    descriptions = {
        'INFO': [
            'User login successful',
            'Database connection established',
            'Request completed successfully',
            'Task scheduled',
            'Service started',
            'Data synchronized',
            'Cache updated',
            'Configuration loaded',
            'API request received',
            'File uploaded successfully'
        ],
        'WARN': [
            'Disk usage 85%',
            'High memory consumption',
            'Slow response time',
            'Connection pool nearing capacity',
            'Rate limit approaching',
            'Deprecated API used',
            'Cache miss rate high',
            'Database connection pool high',
            'Background task taking longer than expected',
            'Session timeout imminent'
        ],
        'ERROR': [
            '500 Internal Server Error',
            'Database connection failed',
            'Invalid authentication token',
            'File not found',
            'Permission denied',
            'Service unavailable',
            'Data validation failed',
            'Network timeout',
            'Out of memory error',
            'API rate limit exceeded'
        ],
        'CRITICAL': [
            'System shutdown required',
            'Database corruption detected',
            'Security breach detected',
            'Disk space critically low',
            'Service completely unresponsive',
            'Fatal error in main thread',
            'Data integrity compromised',
            'Primary-secondary sync failure',
            'Cluster quorum lost',
            'Certificate expired'
        ]
    }
    
    num_entries = random.randint(min_entries, max_entries)
    log_entries = []
    
    # Random choices for log generation
    for _ in range(num_entries):
        timestamp = datetime.datetime.now()
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        log_level = random.choice(log_levels)
        description = random.choice(descriptions[log_level])
        server = random.choice(servers)
        log_entries.append([formatted_timestamp, log_level, description, server])

    log_entries.sort(key=lambda x: x[0])
    timestamp = int(time.time())
    filename = f"logs_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Write to CSV
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter='|')
        for entry in log_entries:
            writer.writerow(entry)
    
    print(f"Generated {num_entries} log entries in {filepath}")
    return filepath

if __name__ == "__main__":
    executions = 4 # Change it to generate different amount of log entries
    for x in range(executions): 
        generate_logs()
        if x == executions - 1:
            break
        time.sleep(10) # Wait for 10 seconds before generating the next file

