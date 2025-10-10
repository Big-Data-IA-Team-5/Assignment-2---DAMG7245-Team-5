FROM apache/airflow:2.8.0-python3.9

USER airflow

# Install Python packages using --user flag
RUN pip install --no-cache-dir --user \
    docling \
    PyMuPDF \
    pandas \
    google-cloud-storage \
    selenium \
    beautifulsoup4 \
    webdriver-manager \
    apache-airflow-providers-google \
    lxml

# Verify installation
RUN python -c "import docling; import fitz; import pandas; import google.cloud.storage; import selenium; import bs4; print('All packages installed!')"
