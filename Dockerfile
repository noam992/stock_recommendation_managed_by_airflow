FROM apache/airflow:2.5.1

USER root

# Install Chrome, ChromeDriver, and X11 dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget \
        gnupg2 \
        xvfb \
        x11vnc \
        xauth \
        xorg \
        openbox \
        libnss3 \
        libgbm1 \
        libasound2 \
        unzip \
        tesseract-ocr \
        libtesseract-dev \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        google-chrome-stable \
    && CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d'.' -f1) \
    && wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROME_VERSION.0.6723.0/linux64/chromedriver-linux64.zip" \
    && unzip chromedriver-linux64.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf chromedriver-linux64.zip chromedriver-linux64 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install X11 and Xvfb with proper permissions
RUN apt-get update && apt-get install -y \
    xvfb \
    x11-utils \
    x11-apps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/.X11-unix \
    && chmod 1777 /tmp/.X11-unix \
    && chmod u+s /usr/bin/Xvfb

# Set up display environment variables
ENV DISPLAY=:99
ENV DISPLAY_WIDTH=1920
ENV DISPLAY_HEIGHT=1080

# Create directory for X11 socket and set permissions
RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt