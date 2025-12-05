import pandas as pd
import os
import glob

def Plotly_Table_To_Df(header, cells):
    """Convert Plotly table JSON to pandas DataFrame."""
    if not header or not cells:
        return pd.DataFrame(columns=['Facility Type', 'Visit Date', 'Average Time Spent'])
    return pd.DataFrame({header[i]: cells[i] for i in range(len(header))})

def Load_Parquet_By_Date(folder, filter_date):
    """Load all parquet files recursively and filter for 7-day window ending at filter_date."""
    folder = os.path.normpath(folder)
    all_files = glob.glob(os.path.join(folder, '**', '*.parquet'), recursive=True)

    frames = []
    for f in all_files:
        try:
            df = pd.read_parquet(f)
            if 'visit_date' not in df.columns or 'facility_type' not in df.columns:
                continue
            df['visit_date'] = pd.to_datetime(df['visit_date'], errors='coerce')
            df['_source'] = 'Parquet'
            frames.append(df)
        except Exception as e:
            print(f"Failed to read {f}: {e}")

    if not frames:
        return pd.DataFrame(columns=['Facility Type', 'Visit Date', 'Average Time Spent', '_source'])

    df = pd.concat(frames, ignore_index=True)

    df = df.rename(columns={
        'facility_type': 'Facility Type',
        'visit_date': 'Visit Date',
        'avg_time_spent': 'Average Time Spent'
    })
    df['Visit Date'] = pd.to_datetime(df['Visit Date']).dt.normalize()

    if filter_date is not None:
        filter_date = pd.to_datetime(filter_date)
        start_date = filter_date - pd.Timedelta(days=6)
        df = df[(df['Visit Date'] >= start_date) & (df['Visit Date'] <= filter_date)]

    df = df.groupby(['Facility Type', 'Visit Date'], as_index=False).agg({'Average Time Spent':'mean'})
    df['_source'] = 'Parquet'

    return df

def Normalize_7Day_Window(df_html, df_parquet, filter_date):
    """Align both HTML and Parquet to same 7-day window and facility types."""
    filter_date = pd.to_datetime(filter_date)
    start_date = filter_date - pd.Timedelta(days=6)
    date_range = pd.date_range(start=start_date, end=filter_date)

    facilities = pd.Series(pd.concat([df_html['facility_type'], df_parquet['facility_type']])).dropna().unique()
    full_index = pd.MultiIndex.from_product([facilities, date_range], names=['facility_type', 'visit_date'])

    def reindex_df(df):
        df = df.groupby(['facility_type', 'visit_date'], as_index=False).agg({'average_time_spent':'mean'})
        df = df.set_index(['facility_type', 'visit_date']).reindex(full_index).reset_index()
        return df

    return reindex_df(df_html), reindex_df(df_parquet)

def Compare_And_Fail(df_html, df_parquet, filter_date=None):
    """Compare HTML vs Parquet and return a readable string of differences."""

    df_html = df_html.rename(columns=lambda x: x.strip().lower().replace(' ', '_'))
    df_parquet = df_parquet.rename(columns=lambda x: x.strip().lower().replace(' ', '_'))

    df_html = df_html.reindex(columns=['facility_type', 'visit_date', 'average_time_spent'], fill_value=pd.NA)
    df_parquet = df_parquet.reindex(columns=['facility_type', 'visit_date', 'average_time_spent', '_source'], fill_value='Parquet')

    df_html['visit_date'] = pd.to_datetime(df_html['visit_date'], errors='coerce').dt.normalize()
    df_parquet['visit_date'] = pd.to_datetime(df_parquet['visit_date'], errors='coerce').dt.normalize()
    df_html['average_time_spent'] = pd.to_numeric(df_html['average_time_spent'], errors='coerce')
    df_parquet['average_time_spent'] = pd.to_numeric(df_parquet['average_time_spent'], errors='coerce')
    df_html['facility_type'] = df_html['facility_type'].astype(str).str.strip()
    df_parquet['facility_type'] = df_parquet['facility_type'].astype(str).str.strip()
    df_html['_source'] = 'HTML'

    if filter_date is not None:
        df_html, df_parquet = Normalize_7Day_Window(df_html, df_parquet, filter_date)

    df_diff = pd.merge(
        df_html,
        df_parquet,
        on=['facility_type', 'visit_date'],
        how='outer',
        suffixes=('_html', '_parquet')
    )

    def display_val(val):
        return f"{val:.2f}" if pd.notna(val) else "MISSING"

    output_rows = []
    for _, row in df_diff.iterrows():
        html_val = display_val(row['average_time_spent_html'])
        parquet_val = display_val(row['average_time_spent_parquet'])
        if html_val != parquet_val:
            output_rows.append({
                'Facility': row['facility_type'],
                'Date': row['visit_date'].strftime('%Y-%m-%d'),
                'HTML': html_val,
                'Parquet': parquet_val
            })

    if output_rows:
        df_out = pd.DataFrame(output_rows)
        diff_string = "\n***** Differences Detected *****\n" + df_out.to_string(index=False)
        raise AssertionError(diff_string)

    return pd.DataFrame(output_rows)
