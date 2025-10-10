FROM apache/airflow:2.8.0-python3.9

# Switch to root to install system packages
USER root

    # Install Chromium (works on ARM64) and ChromeDriver
    RUN apt-get update && apt-get install -y \
        wget \
        gnupg2 \
        ca-certificates \
        unzip \
        chromium \
        chromium-driver \
        --no-install-recommends \
        && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python packages (using prebuilt wheels to avoid compilation)
RUN pip install --no-cache-dir --user \
    docling \
    pandas \
    google-cloud-storage \
    selenium \
    beautifulsoup4 \
    webdriver-manager \
    apache-airflow-providers-google \
    lxml

# Verify installation (PyMuPDF is included with docling)
RUN python -c "import docling; import pandas; import google.cloud.storage; import selenium; import bs4; print('All packages installed!')"
