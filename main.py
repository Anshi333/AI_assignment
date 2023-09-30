from flask import Flask, render_template, send_from_directory,jsonify
import file1
from file1 import ua_df,logs_df
import base64 
from io import BytesIO

app = Flask(__name__)

def fetch_analysis_results(logs_df,ua_df):
    top10_hits_per_ip_address, ip_address_plot = file1.get_top10_hits_per_ip_address(logs_df)
    top10_hits_per_http_codes, http_codes_plot = file1.get_top10_hits_per_http_codes(logs_df)
    top10_hits_per_URL, URL_plot = file1.get_top10_hits_per_URL(logs_df)
    top10_hits_per_Platform, Platform_plot = file1.get_top10_hits_per_Platform(ua_df)
    top10_hits_per_Browser,Browser_plot = file1.get_top10_hits_per_Browser(ua_df)
    top10_hits_per_hr, Hr_plot = file1.get_top10_hits_per_hr(logs_df)
    return {
        'head_of_log_file': file1.get_logfile_head(logs_df),
        'tail_of_log_file': file1.get_logfile_tail(logs_df),
        'total_records': file1.get_total_records(logs_df),
        'total_columns': file1.get_total_columns(logs_df),
        'unique_ip_addresses': file1.get_unique_ip_addresses(logs_df),
        'request_count_per_ip': file1.get_request_count_per_ip(logs_df),
        'top10_hits_per_ip_address': top10_hits_per_ip_address,
        'ip_address_plot': ip_address_plot,
        'request_count_per_httpcode': file1.get_request_count_per_httpcode(logs_df),
        'top10_hits_per_http_codes': top10_hits_per_http_codes,
        'http_codes_plot' : http_codes_plot,
        'unique_http_codes': file1.get_unique_http_codes(logs_df),
        'request_count_per_URL': file1.get_request_count_per_URL(logs_df),
        'URL_max_hits': file1.get_URL_max_hits(logs_df),
        'top10_hits_per_URL': top10_hits_per_URL,
        'URL_plot' : URL_plot,
        'request_count_per_Platform': file1.get_request_count_per_Platform(ua_df),
        'top10_hits_per_Platform': top10_hits_per_Platform,
        'Platform_plot' : Platform_plot,
        'request_count_per_Browser': file1.get_request_count_per_Browser(ua_df),
        'top10_hits_per_Browser': top10_hits_per_Browser,
        'Browser_plot' : Browser_plot,
        'hits_count_per_hr': file1.get_hits_count_per_hr(logs_df),
        'top10_hits_per_hr': top10_hits_per_hr,
        'Hr_plot' : Hr_plot,
        'traffic_distribution_hrly': file1.get_traffic_distribution_hrly(logs_df),
        
    }


@app.route('/')
def index():
    analysis_results = fetch_analysis_results(logs_df, ua_df)
    return render_template('index.html', analysis_results=analysis_results)


if __name__ == '__main__':
    app.run(debug=True)

