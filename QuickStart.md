 Works on **Windows PowerShell**. Creates a local virtual environment, installs deps, runs the full pipeline, then starts the Streamlit dashboard.

### 1) Clone + enter project
git clone <YOUR_REPO_URL>
cd retailer-kpi-dashboard-pipeline

### 2) Create + activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

### 3) Install dependencies
pip install -r requirements.txt

### 4) Run the pipeline (end-to-end)
- python pipeline\extract\01_build_static_table.py
- python pipeline\transform\01_build_retailer_year_facts.py
- python pipeline\model\01_compute_kpis.py
- python pipeline\quality_engine\03_flag_anomalies_and_readiness.py
- python pipeline\model\02_apply_confidence_weighting.py

### 5) Launch the dashboard
streamlit run pipeline\dashboard\app.py
